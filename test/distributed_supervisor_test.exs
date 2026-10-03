defmodule Test.Service.DistributedSupervisorTest do
  alias ProcessHub.Utility.Bag
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias ProcessHub.Future

  use ExUnit.Case

  @hub_id :dist_sup_test

  setup _context do
    Test.Helper.SetupHelper.setup_base(%{}, @hub_id)
  end

  test "process self shutdown", %{hub: hub} do
    handler = Bag.recv_hook(Hook.child_unregistered(), self())

    HookManager.register_handler(hub.storage.hook, Hook.child_unregistered(), handler)

    child_spec1 = %{
      id: :self_shutdown_1,
      start: {Test.Helper.TestServer, :start_link, [%{name: :self_shutdown_1}]},
      restart: :transient
    }

    child_spec2 = %{
      id: :self_shutdown_2,
      start: {Test.Helper.TestServer, :start_link, [%{name: :self_shutdown_2}]},
      restart: :transient
    }

    ProcessHub.start_children(@hub_id, [child_spec1, child_spec2],
      awaitable: true,
      timeout: 4000
    )
    |> Future.await()

    GenServer.cast(:self_shutdown_1, {:stop, :normal})
    GenServer.cast(:self_shutdown_2, {:stop, :shutdown})

    Bag.receive_multiple(2, Hook.child_unregistered())

    # Make sure the child has been removed from registry
    assert ProcessHub.process_list(@hub_id, :global) === []

    # Make sure the child specification has been removed from the supervisor
    assert Supervisor.which_children(hub.procs.dist_sup) === []
  end

  test "self shutdown cannot deadlock a concurrent child census", %{hub: hub} do
    child_id = :self_shutdown_during_census

    child_spec = %{
      id: child_id,
      start: {Test.Helper.TestServer, :start_link, [%{name: child_id}]},
      restart: :transient
    }

    assert %ProcessHub.StartResult{status: :ok} =
             ProcessHub.start_child(@hub_id, child_spec, awaitable: true)
             |> Future.await()

    child_pid = ProcessHub.get_pid(@hub_id, child_id)
    dist_sup = hub.procs.dist_sup
    dist_sup_pid = GenServer.whereis(dist_sup)

    # Fix the mailbox order which exposed the field failure: the child's EXIT is
    # ahead of the coordinator's synchronous Supervisor.which_children/1 call.
    :ok = :sys.suspend(dist_sup)
    :ok = GenServer.stop(child_pid, :normal)
    census = Task.async(fn -> ProcessHub.which_children(@hub_id, [:local]) end)

    assert eventually_message?(dist_sup_pid, fn
             {:EXIT, ^child_pid, :normal} -> true
             _ -> false
           end)

    assert eventually_message?(dist_sup_pid, fn
             {:"$gen_call", _from, :which_children} -> true
             _ -> false
           end)

    :ok = :sys.resume(dist_sup)

    assert {_, _children} = Task.await(census, 2_000)

    assert Test.Helper.Common.eventually(fn ->
             dist_sup
             |> Supervisor.which_children()
             |> Enum.all?(fn {id, _, _, _} -> id !== child_id end)
           end)

    assert Test.Helper.Common.eventually(fn -> ProcessHub.get_pid(@hub_id, child_id) == nil end)
  end

  test "terminate_child stops and removes child from supervisor", %{hub: hub} do
    child_spec = %{
      id: :term_child_test,
      start: {Test.Helper.TestServer, :start_link, [%{name: :term_child_test}]}
    }

    {:ok, pid} = ProcessHub.DistributedSupervisor.start_child(hub.procs.dist_sup, child_spec)

    assert Process.alive?(pid)

    # Verify child is in the supervisor
    children = Supervisor.which_children(hub.procs.dist_sup)
    assert length(children) === 1
    assert Enum.any?(children, fn {id, _, _, _} -> id === :term_child_test end)

    # Terminate the child
    result =
      ProcessHub.DistributedSupervisor.terminate_child(hub.procs.dist_sup, :term_child_test)

    assert result === :ok

    # Verify process is no longer alive
    refute Process.alive?(pid)

    # Verify child is fully removed from supervisor (not just stopped)
    assert Supervisor.which_children(hub.procs.dist_sup) === []
  end

  test "start_link accepts the documented hub-id tuple" do
    supervisor_name = Test.Helper.SetupHelper.unique_id(:standalone_dist_sup)

    assert {:ok, supervisor} =
             ProcessHub.DistributedSupervisor.start_link({@hub_id, supervisor_name, 3, 5})

    assert Process.alive?(supervisor)
    assert Supervisor.which_children(supervisor) == []
    assert :ok = GenServer.stop(supervisor)
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

  test "terminate_child returns error for non-existent child", %{hub: hub} do
    result =
      ProcessHub.DistributedSupervisor.terminate_child(hub.procs.dist_sup, :nonexistent_child)

    assert result === {:error, :not_found}
  end

  test "start_child adds child to supervisor", %{hub: hub} do
    child_spec = %{
      id: :start_child_test,
      start: {Test.Helper.TestServer, :start_link, [%{name: :start_child_test}]}
    }

    assert Supervisor.which_children(hub.procs.dist_sup) === []

    {:ok, pid} = ProcessHub.DistributedSupervisor.start_child(hub.procs.dist_sup, child_spec)

    assert is_pid(pid)
    assert Process.alive?(pid)

    children = Supervisor.which_children(hub.procs.dist_sup)
    assert length(children) === 1

    [{child_id, child_pid, type, _modules}] = children
    assert child_id === :start_child_test
    assert child_pid === pid
    assert type === :worker
  end

  test "start_child returns error for duplicate child id", %{hub: hub} do
    child_spec = %{
      id: :dup_child_test,
      start: {Test.Helper.TestServer, :start_link, [%{name: :dup_child_test}]}
    }

    {:ok, _pid} = ProcessHub.DistributedSupervisor.start_child(hub.procs.dist_sup, child_spec)

    # Starting same child id again should fail
    result = ProcessHub.DistributedSupervisor.start_child(hub.procs.dist_sup, child_spec)

    assert {:error, {:already_started, _pid}} = result
  end

  test "multiple children can be started and terminated", %{hub: hub} do
    specs =
      for i <- 1..5 do
        name = :"multi_test_#{i}"
        %{id: name, start: {Test.Helper.TestServer, :start_link, [%{name: name}]}}
      end

    pids =
      Enum.map(specs, fn spec ->
        {:ok, pid} = ProcessHub.DistributedSupervisor.start_child(hub.procs.dist_sup, spec)
        pid
      end)

    assert Enum.all?(pids, &Process.alive?/1)
    assert length(Supervisor.which_children(hub.procs.dist_sup)) === 5

    # Terminate all
    Enum.each(specs, fn spec ->
      :ok = ProcessHub.DistributedSupervisor.terminate_child(hub.procs.dist_sup, spec.id)
    end)

    assert Supervisor.which_children(hub.procs.dist_sup) === []
    refute Enum.any?(pids, &Process.alive?/1)
  end
end
