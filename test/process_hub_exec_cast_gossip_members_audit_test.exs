defmodule Test.ProcessHubExecCastGossipMembersAuditTest do
  use ExUnit.Case, async: false

  alias ProcessHub.Constant.StorageKey
  alias ProcessHub.Service.Storage
  alias ProcessHub.Strategy.Synchronization.Base, as: SynchronizationStrategy
  alias ProcessHub.Strategy.Synchronization.Gossip

  setup do
    hub_id = Test.Helper.SetupHelper.unique_id(:gossip_members_audit)
    misc = :ets.new(hub_id, [:set, :public])
    hook = :ets.new(:gossip_members_hook, [:set, :public])
    hub = %ProcessHub.Hub{hub_id: hub_id, storage: %{misc: misc, hook: hook}}
    Storage.insert(misc, StorageKey.hn(), [node()])

    Process.register(self(), hub_id)

    on_exit(fn ->
      if Process.whereis(hub_id), do: Process.unregister(hub_id)
      if :ets.info(misc) != :undefined, do: :ets.delete(misc)
      if :ets.info(hook) != :undefined, do: :ets.delete(hook)
    end)

    %{hub: hub}
  end

  test "external propagation excludes the local coordinator", %{hub: hub} do
    assert :ok =
             SynchronizationStrategy.propagate(%Gossip{}, hub, [:external_request],
               members: :external
             )

    refute_receive {_event, [:external_request]}, 100
  end

  test "local propagation includes the local coordinator", %{hub: hub} do
    assert :ok =
             SynchronizationStrategy.propagate(%Gossip{}, hub, [:local_request], members: :local)

    assert_receive {_event, [:local_request]}, 100
  end
end
