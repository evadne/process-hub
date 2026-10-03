defmodule Test.ProcessHubExecCastLifecycleAuditTest do
  use ExUnit.Case, async: false

  alias ProcessHub.Future
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
