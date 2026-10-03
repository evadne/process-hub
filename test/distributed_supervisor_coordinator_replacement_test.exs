defmodule Test.Service.DistributedSupervisorCoordinatorReplacementTest do
  use ExUnit.Case, async: false

  alias ProcessHub.Future
  alias Test.Helper.Common
  alias Test.Helper.SetupHelper

  test "a normal transient retirement survives coordinator replacement" do
    hub_id = SetupHelper.unique_id(:coordinator_replacement)
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
end
