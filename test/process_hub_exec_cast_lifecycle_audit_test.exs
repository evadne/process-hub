defmodule Test.ProcessHubExecCastLifecycleAuditTest do
  use ExUnit.Case, async: false

  alias ProcessHub.Future
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias ProcessHub.Service.ProcessRegistry
  alias Test.Helper.Common
  alias Test.Helper.SetupHelper

  setup do
    hub_id = SetupHelper.unique_id(:exec_cast_lifecycle)
    {_hub_id, _initializer} = SetupHelper.start_hub!(hub_id: hub_id)
    %{hub_id: hub_id, hub: ProcessHub.Coordinator.get_hub(hub_id)}
  end

  test "transient abnormal exit restarts and publishes the replacement", context do
    child_id = SetupHelper.unique_id(:transient_abnormal)
    spec = spec(child_id, :transient)
    old_pid = start_child!(context.hub_id, spec)

    :ok = GenServer.stop(old_pid, :audit_failure)

    assert Common.eventually(fn ->
             case ProcessHub.get_pid(context.hub_id, child_id) do
               pid when is_pid(pid) -> pid != old_pid and Process.alive?(pid)
               _ -> false
             end
           end)

    new_pid = ProcessHub.get_pid(context.hub_id, child_id)
    assert {^spec, [{local_node, ^new_pid}]} = ProcessRegistry.lookup(context.hub_id, child_id)
    assert local_node == node()
  end

  test "permanent normal exit restarts and publishes the replacement", context do
    child_id = SetupHelper.unique_id(:permanent_normal)
    spec = spec(child_id, :permanent)
    old_pid = start_child!(context.hub_id, spec)

    :ok = GenServer.stop(old_pid, :normal)

    assert Common.eventually(fn ->
             case ProcessHub.get_pid(context.hub_id, child_id) do
               pid when is_pid(pid) -> pid != old_pid and Process.alive?(pid)
               _ -> false
             end
           end)

    new_pid = ProcessHub.get_pid(context.hub_id, child_id)
    assert {^spec, [{local_node, ^new_pid}]} = ProcessRegistry.lookup(context.hub_id, child_id)
    assert local_node == node()
  end

  test "temporary abnormal exit removes the spec and registry route", context do
    child_id = SetupHelper.unique_id(:temporary_abnormal)
    old_pid = start_child!(context.hub_id, spec(child_id, :temporary))

    :ok = GenServer.stop(old_pid, :audit_failure)

    assert_removed(context, child_id)
  end

  test "transient normal exit removes the spec and registry route", context do
    child_id = SetupHelper.unique_id(:transient_normal)
    old_pid = start_child!(context.hub_id, spec(child_id, :transient))

    :ok = GenServer.stop(old_pid, :normal)

    assert_removed(context, child_id)
  end

  test "a same-ID ensure queued before retirement succeeds after synchronous cleanup",
       context do
    child_id = SetupHelper.unique_id(:rapid_reensure)
    spec = spec(child_id, :transient)
    old_pid = start_child!(context.hub_id, spec)
    coordinator = GenServer.whereis(context.hub_id)

    :ok = :sys.suspend(coordinator)

    ensure_task =
      Task.async(fn ->
        ProcessHub.start_child(context.hub_id, spec, awaitable: true, call_timeout: 5_000)
        |> Future.await()
      end)

    assert eventually_message?(coordinator, fn
             {:"$gen_call", _from, {:init_children_start, [queued], _opts}} ->
               queued.id == child_id

             _ ->
               false
           end)

    :ok = GenServer.stop(old_pid, :normal)

    assert Common.eventually(fn -> child_absent?(context.hub.procs.dist_sup, child_id) end)

    assert Common.eventually(fn -> ProcessHub.get_pid(context.hub_id, child_id) == nil end)

    :ok = :sys.resume(coordinator)

    assert %ProcessHub.StartResult{status: :ok} = Task.await(ensure_task, 5_000)
    replacement = ProcessHub.get_pid(context.hub_id, child_id)
    assert replacement != old_pid
    assert is_pid(replacement) and Process.alive?(replacement)
  end

  test "normal retirement hooks may synchronously inspect the coordinator and supervisor",
       context do
    hub_id = context.hub_id
    child_id = SetupHelper.unique_id(:reentrant_removal_hook)
    old_pid = start_child!(context.hub_id, spec(child_id, :transient))
    register_reentrant_hook(context, Hook.child_unregistered(), :removal)

    :ok = GenServer.stop(old_pid, :normal)

    assert_receive {:lifecycle_hook, :removal, %{child_id: ^child_id}, ^hub_id, child_count},
                   1_000

    assert is_integer(child_count)
    assert_removed(context, child_id)
  end

  test "restart hooks may synchronously inspect the coordinator and supervisor", context do
    hub_id = context.hub_id
    child_id = SetupHelper.unique_id(:reentrant_restart_hook)
    old_pid = start_child!(context.hub_id, spec(child_id, :transient))
    register_reentrant_hook(context, Hook.child_pid_updated(), :restart)

    :ok = GenServer.stop(old_pid, :audit_failure)

    assert_receive {:lifecycle_hook, :restart, %{node: local_node, pid: new_pid}, ^hub_id,
                    child_count},
                   1_000

    assert local_node == node()
    assert is_pid(new_pid) and new_pid != old_pid
    assert is_integer(child_count)
    assert Common.eventually(fn -> ProcessHub.get_pid(context.hub_id, child_id) == new_pid end)
  end

  test "normal retirement does not announce removal when its registry row is already absent",
       context do
    child_id = SetupHelper.unique_id(:absent_removal_hook)
    old_pid = start_child!(context.hub_id, spec(child_id, :transient))
    register_reentrant_hook(context, Hook.child_unregistered(), :absent_removal)

    :ok = ProcessRegistry.delete(context.hub_id, child_id)
    :ok = GenServer.stop(old_pid, :normal)

    refute_receive {:lifecycle_hook, :absent_removal, _, _, _}, 100
    assert_removed(context, child_id)
  end

  def reentrant_lifecycle_hook(owner, hub_id, dist_sup, kind, hook_data) do
    hub = ProcessHub.Coordinator.get_hub(hub_id)
    children = Supervisor.which_children(dist_sup)
    send(owner, {:lifecycle_hook, kind, hook_data, hub.hub_id, length(children)})
    :ok
  end

  defp start_child!(hub_id, child_spec) do
    result =
      ProcessHub.start_child(hub_id, child_spec, awaitable: true, timeout: 2_000)
      |> Future.await()

    assert %ProcessHub.StartResult{status: :ok} = result
    ProcessHub.StartResult.pid(result)
  end

  defp assert_removed(context, child_id) do
    assert Common.eventually(fn -> child_absent?(context.hub.procs.dist_sup, child_id) end)
    assert Common.eventually(fn -> ProcessHub.get_pid(context.hub_id, child_id) == nil end)
    assert ProcessRegistry.lookup(context.hub_id, child_id) == nil
  end

  defp register_reentrant_hook(context, hook, kind) do
    handler = %HookManager{
      id: SetupHelper.unique_id(kind),
      m: __MODULE__,
      f: :reentrant_lifecycle_hook,
      a: [self(), context.hub_id, context.hub.procs.dist_sup, kind, :_]
    }

    assert :ok = HookManager.register_handler(context.hub.storage.hook, hook, handler)
  end

  defp child_absent?(dist_sup, child_id) do
    dist_sup
    |> Supervisor.which_children()
    |> Enum.all?(fn {id, _pid, _type, _modules} -> id != child_id end)
  end

  defp spec(child_id, restart) do
    %{
      id: child_id,
      start: {Test.Helper.TestServer, :start_link, [%{name: child_id}]},
      restart: restart
    }
  end

  defp eventually_message?(pid, matcher, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_for_message(pid, matcher, deadline)
  end

  defp wait_for_message(pid, matcher, deadline) do
    messages = Process.info(pid, :messages) |> elem(1)

    cond do
      Enum.any?(messages, matcher) ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(5)
        wait_for_message(pid, matcher, deadline)
    end
  end
end
