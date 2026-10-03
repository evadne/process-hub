defmodule Test.ProcessHubExecCastCoordinatorDeliveryRegressionTest do
  use ExUnit.Case, async: false

  alias ProcessHub.Future
  alias Test.Helper.Common
  alias Test.Helper.SetupHelper

  test "a normal transient retirement survives coordinator replacement" do
    hub_id = SetupHelper.unique_id(:exec_cast_coordinator_race)
    {_hub_id, initializer} = SetupHelper.start_hub!(hub_id: hub_id)
    hub = ProcessHub.Coordinator.get_hub(hub_id)
    child_id = SetupHelper.unique_id(:coordinator_race_child)

    spec = %{
      id: child_id,
      start: {Test.Helper.TestServer, :start_link, [%{name: child_id}]},
      restart: :transient
    }

    assert %ProcessHub.StartResult{status: :ok} =
             ProcessHub.start_child(hub_id, spec, awaitable: true, timeout: 2_000)
             |> Future.await()

    old_child = ProcessHub.get_pid(hub_id, child_id)
    old_coordinator = GenServer.whereis(hub_id)
    :ok = :sys.suspend(old_coordinator)
    :ok = GenServer.stop(old_child, :normal)

    assert Common.eventually(fn ->
             hub.procs.dist_sup
             |> Supervisor.which_children()
             |> Enum.all?(fn {id, _pid, _type, _modules} -> id != child_id end)
           end)

    assert Common.eventually(fn -> ProcessHub.get_pid(hub_id, child_id) == nil end)

    Process.exit(old_coordinator, :kill)

    assert Common.eventually(fn ->
             case GenServer.whereis(hub_id) do
               pid when is_pid(pid) -> pid != old_coordinator and Process.alive?(pid)
               _ -> false
             end
           end)

    # The retirement was committed without the suspended coordinator. Replacing
    # that coordinator cannot resurrect or retain the old route.
    assert Common.eventually(fn -> ProcessHub.get_pid(hub_id, child_id) == nil end, 1_000)
    assert Supervisor.which_children(hub.procs.dist_sup) == []
    assert Process.alive?(initializer)
  end

  test "a queued same-ID replacement cannot be erased by the preceding retirement" do
    hub_id = SetupHelper.unique_id(:exec_cast_same_id_race)
    {_hub_id, _initializer} = SetupHelper.start_hub!(hub_id: hub_id)
    hub = ProcessHub.Coordinator.get_hub(hub_id)
    child_id = SetupHelper.unique_id(:same_id_child)

    spec = %{
      id: child_id,
      start: {Test.Helper.TestServer, :start_link, [%{name: child_id}]},
      restart: :transient
    }

    assert %ProcessHub.StartResult{status: :ok} =
             ProcessHub.start_child(hub_id, spec, awaitable: true, timeout: 2_000)
             |> Future.await()

    old_child = ProcessHub.get_pid(hub_id, child_id)
    coordinator = GenServer.whereis(hub_id)
    :ok = :sys.suspend(coordinator)

    replacement_task =
      Task.async(fn ->
        ProcessHub.start_child(hub_id, spec,
          awaitable: true,
          check_existing: false,
          call_timeout: 5_000
        )
        |> Future.await()
      end)

    assert eventually_message?(coordinator, fn
             {:"$gen_call", _from, {:init_children_start, [queued], _opts}} ->
               queued.id == child_id

             _ ->
               false
           end)

    :ok = GenServer.stop(old_child, :normal)

    assert Common.eventually(fn ->
             hub.procs.dist_sup
             |> Supervisor.which_children()
             |> Enum.all?(fn {id, _pid, _type, _modules} -> id != child_id end)
           end)

    assert Common.eventually(fn -> ProcessHub.get_pid(hub_id, child_id) == nil end)

    :ok = :sys.resume(coordinator)
    assert %ProcessHub.StartResult{status: :ok} = Task.await(replacement_task, 5_000)

    assert Common.eventually(fn ->
             case Supervisor.which_children(hub.procs.dist_sup) do
               [{^child_id, pid, _type, _modules}] when is_pid(pid) -> pid != old_child
               _ -> false
             end
           end)

    [{^child_id, replacement_pid, _type, _modules}] =
      Supervisor.which_children(hub.procs.dist_sup)

    # Desired invariant: the old removal must be generation-fenced so it cannot
    # erase the registry route installed for this replacement.
    assert Common.eventually(fn ->
             ProcessHub.get_pid(hub_id, child_id) == replacement_pid
           end)
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
