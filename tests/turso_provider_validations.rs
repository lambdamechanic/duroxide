//! Provider validation tests for Turso
//!
//! This test file validates the Turso provider using the reusable
//! provider validation test suite from `duroxide::provider_validations`.
//!
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]

//! These tests automatically enable the `provider-test` feature when running
//! tests within the duroxide repository.

#[cfg(all(feature = "provider-test", feature = "turso"))]
mod tests {
    use duroxide::provider_validations::sessions::{
        test_abandoned_session_item_ignore_attempt, test_abandoned_session_item_retryable,
        test_ack_updates_session_last_activity, test_activity_lock_expires_session_lock_valid_same_worker_refetches,
        test_both_locks_expire_different_worker_claims, test_cleanup_keeps_active_sessions,
        test_cleanup_keeps_sessions_with_pending_items, test_cleanup_removes_expired_no_items,
        test_cleanup_then_new_item_recreates_session, test_concurrent_session_claim_only_one_wins,
        test_different_sessions_different_workers, test_mixed_session_and_non_session_items,
        test_non_session_items_fetchable_by_any_worker, test_non_session_items_returned_with_session_config,
        test_none_session_skips_session_items, test_original_worker_reclaims_expired_session,
        test_renew_session_lock_active, test_renew_session_lock_after_expiry_returns_zero,
        test_renew_session_lock_no_sessions, test_renew_session_lock_skips_idle,
        test_renew_work_item_updates_session_last_activity, test_session_affinity_blocks_other_worker,
        test_session_affinity_same_worker, test_session_claimable_after_lock_expiry,
        test_session_item_claimable_when_no_session, test_session_items_processed_in_order,
        test_session_lock_expires_activity_lock_valid_ack_succeeds,
        test_session_lock_expires_new_owner_gets_redelivery, test_session_lock_expires_same_worker_reacquires,
        test_session_lock_renewal_extends_past_original_timeout, test_session_takeover_after_lock_expiry,
        test_shared_worker_id_any_caller_can_fetch_owned_session, test_some_session_returns_all_items,
    };
    use duroxide::provider_validations::tag_filtering::{
        test_any_filter_fetches_everything, test_default_and_fetches_untagged_and_matching,
        test_default_only_fetches_untagged, test_multi_runtime_tag_isolation, test_multi_tag_filter,
        test_none_filter_returns_nothing, test_tag_preserved_through_ack_orchestration_item,
        test_tag_round_trip_preservation, test_tag_survives_abandon_and_refetch, test_tags_fetches_only_matching,
    };
    use duroxide::provider_validations::{
        ProviderFactory,
        // Bulk deletion tests
        bulk_deletion::{
            test_delete_instance_bulk_cascades_to_children, test_delete_instance_bulk_completed_before_filter,
            test_delete_instance_bulk_filter_combinations, test_delete_instance_bulk_safety_and_limits,
        },
        // Capability filtering tests
        capability_filtering::{
            test_ack_stores_pinned_version_via_metadata_update, test_concurrent_filtered_fetch_no_double_lock,
            test_continue_as_new_execution_gets_own_pinned_version, test_fetch_filter_boundary_versions,
            test_fetch_filter_does_not_lock_skipped_instances, test_fetch_filter_null_pinned_version_always_compatible,
            test_fetch_filter_skips_incompatible_selects_compatible, test_fetch_single_range_only_uses_first_range,
            test_fetch_with_compatible_filter_returns_item, test_fetch_with_filter_none_returns_any_item,
            test_fetch_with_incompatible_filter_skips_item, test_filter_with_empty_supported_versions_returns_nothing,
            test_pinned_version_immutable_across_ack_cycles, test_pinned_version_stored_via_ack_metadata,
            test_provider_updates_pinned_version_when_told,
        },
        // Deletion tests
        deletion::{
            test_cascade_delete_hierarchy, test_delete_cleans_queues_and_locks, test_delete_get_instance_tree,
            test_delete_get_parent_id, test_delete_instances_atomic, test_delete_instances_atomic_force,
            test_delete_instances_atomic_orphan_detection, test_delete_nonexistent_instance,
            test_delete_running_rejected_force_succeeds, test_delete_terminal_instances,
            test_force_delete_prevents_ack_recreation, test_list_children, test_stale_activity_after_delete_recreate,
        },
        // Long polling tests
        long_polling::{
            test_fetch_respects_timeout_upper_bound, test_short_poll_returns_immediately,
            test_short_poll_work_item_returns_immediately,
        },
        // Poison message tests
        poison_message::{
            abandon_orchestration_item_ignore_attempt_decrements, abandon_work_item_ignore_attempt_decrements,
            attempt_count_is_per_message, ignore_attempt_never_goes_negative, max_attempt_count_across_message_batch,
            orchestration_attempt_count_increments_on_refetch, orchestration_attempt_count_starts_at_one,
            worker_attempt_count_increments_on_lock_expiry, worker_attempt_count_starts_at_one,
        },
        // Prune tests
        prune::{
            test_prune_bulk, test_prune_bulk_includes_running_instances, test_prune_options_combinations,
            test_prune_safety,
        },
        test_abandon_releases_lock_immediately,
        test_abandon_work_item_releases_lock,
        test_abandon_work_item_with_delay,
        test_ack_only_affects_locked_messages,
        // Cancellation tests
        test_ack_work_item_fails_when_entry_deleted,
        test_ack_work_item_none_deletes_without_enqueue,
        // Atomicity tests
        test_atomicity_failure_rollback,
        test_batch_cancellation_deletes_multiple_activities,
        test_cancelled_activities_deleted_from_worker_queue,
        test_cancelling_nonexistent_activities_is_idempotent,
        test_completions_arriving_during_lock_blocked,
        test_concurrent_ack_prevention,
        test_concurrent_instance_fetching,
        test_concurrent_lock_attempts_respect_expiration,
        test_continue_as_new_creates_new_execution,
        test_corrupted_serialization_data,
        test_cross_instance_lock_isolation,
        test_duplicate_event_id_rejection,
        // Instance locking tests
        test_exclusive_instance_lock,
        test_execution_history_persistence,
        test_execution_id_sequencing,
        // Multi-execution tests
        test_execution_isolation,
        test_fetch_returns_missing_state_when_instance_deleted,
        test_fetch_returns_running_state_for_active_orchestration,
        test_fetch_returns_terminal_state_when_orchestration_completed,
        test_fetch_returns_terminal_state_when_orchestration_continued_as_new,
        test_fetch_returns_terminal_state_when_orchestration_failed,
        test_get_execution_info,
        test_get_instance_info,
        test_get_instance_stats_carry_forward,
        test_get_instance_stats_history,
        test_get_instance_stats_kv,
        test_get_instance_stats_kv_delta_only,
        test_get_instance_stats_kv_merged,
        test_get_instance_stats_nonexistent,
        test_get_queue_depths,
        test_get_system_metrics,
        // Instance creation tests
        test_instance_creation_via_metadata,
        // Error handling tests
        test_invalid_lock_token_on_ack,
        test_invalid_lock_token_rejection,
        test_latest_execution_detection,
        test_list_executions,
        // Management tests
        test_list_instances,
        test_list_instances_by_status,
        test_lock_expiration_during_ack,
        // Lock expiration tests
        test_lock_expires_after_timeout,
        test_lock_released_only_on_successful_ack,
        test_lock_renewal_on_ack,
        test_lock_token_uniqueness,
        test_lost_lock_token_handling,
        test_message_tagging_during_lock,
        test_missing_instance_metadata,
        test_multi_operation_atomic_ack,
        test_multi_threaded_lock_contention,
        test_multi_threaded_lock_expiration_recovery,
        test_multi_threaded_no_duplicate_processing,
        test_no_instance_creation_on_enqueue,
        test_null_version_handling,
        test_orchestration_lock_renewal_after_expiration,
        test_orphan_activity_after_instance_force_deletion,
        test_orphan_queue_messages_dropped,
        test_renew_fails_when_entry_deleted,
        test_renew_returns_missing_when_instance_deleted,
        test_renew_returns_running_when_orchestration_active,
        test_renew_returns_terminal_when_orchestration_completed,
        test_same_activity_in_worker_items_and_cancelled_is_noop,
        test_sub_orchestration_instance_creation,
        test_timer_delayed_visibility,
        test_worker_ack_atomicity,
        test_worker_ack_fails_after_lock_expiry,
        test_worker_delayed_visibility_skips_future_items,
        test_worker_item_immediate_visibility,
        // Worker lock renewal tests
        test_worker_lock_renewal_after_ack,
        test_worker_lock_renewal_after_expiration,
        test_worker_lock_renewal_extends_timeout,
        test_worker_lock_renewal_invalid_token,
        test_worker_lock_renewal_success,
        test_worker_peek_lock_semantics,
        // Queue semantics tests
        test_worker_queue_fifo_ordering,
    };
    use duroxide::providers::turso::TursoProvider;
    use duroxide::providers::{ExecutionMetadata, Provider, ProviderAdmin, WorkItem};
    use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
    use std::sync::Arc;
    use std::time::Duration;

    const TEST_LOCK_TIMEOUT: Duration = Duration::from_millis(1000);

    /// Standard test factory — each `create_provider()` call gets a fresh in-memory DB.
    /// Used by the vast majority of tests that don't need direct DB manipulation.
    struct TursoTestFactory;

    #[async_trait::async_trait]
    impl ProviderFactory for TursoTestFactory {
        async fn create_provider(&self) -> Arc<dyn Provider> {
            Arc::new(TursoProvider::new_in_memory().await.unwrap())
        }

        fn lock_timeout(&self) -> Duration {
            TEST_LOCK_TIMEOUT
        }
    }

    // Atomicity tests
    #[tokio::test]
    async fn test_turso_atomicity_failure_rollback() {
        test_atomicity_failure_rollback(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_multi_operation_atomic_ack() {
        test_multi_operation_atomic_ack(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_lock_released_only_on_successful_ack() {
        test_lock_released_only_on_successful_ack(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_concurrent_ack_prevention() {
        test_concurrent_ack_prevention(&TursoTestFactory).await;
    }

    // Error handling tests
    #[tokio::test]
    async fn test_turso_invalid_lock_token_on_ack() {
        test_invalid_lock_token_on_ack(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_duplicate_event_id_rejection() {
        test_duplicate_event_id_rejection(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_missing_instance_metadata() {
        test_missing_instance_metadata(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_corrupted_serialization_data() {
        test_corrupted_serialization_data(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_lock_expiration_during_ack() {
        test_lock_expiration_during_ack(&TursoTestFactory).await;
    }

    // Instance locking tests
    #[tokio::test]
    async fn test_turso_exclusive_instance_lock() {
        test_exclusive_instance_lock(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_lock_token_uniqueness() {
        test_lock_token_uniqueness(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_invalid_lock_token_rejection() {
        test_invalid_lock_token_rejection(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_concurrent_instance_fetching() {
        test_concurrent_instance_fetching(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_completions_arriving_during_lock_blocked() {
        test_completions_arriving_during_lock_blocked(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_cross_instance_lock_isolation() {
        test_cross_instance_lock_isolation(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_message_tagging_during_lock() {
        test_message_tagging_during_lock(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_ack_only_affects_locked_messages() {
        test_ack_only_affects_locked_messages(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_multi_threaded_lock_contention() {
        test_multi_threaded_lock_contention(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_multi_threaded_no_duplicate_processing() {
        test_multi_threaded_no_duplicate_processing(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_multi_threaded_lock_expiration_recovery() {
        test_multi_threaded_lock_expiration_recovery(&TursoTestFactory).await;
    }

    // Lock expiration tests
    #[tokio::test]
    async fn test_turso_lock_expires_after_timeout() {
        test_lock_expires_after_timeout(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_abandon_releases_lock_immediately() {
        test_abandon_releases_lock_immediately(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_lock_renewal_on_ack() {
        test_lock_renewal_on_ack(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_concurrent_lock_attempts_respect_expiration() {
        test_concurrent_lock_attempts_respect_expiration(&TursoTestFactory).await;
    }

    // Multi-execution tests
    #[tokio::test]
    async fn test_turso_execution_isolation() {
        test_execution_isolation(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_latest_execution_detection() {
        test_latest_execution_detection(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_execution_id_sequencing() {
        test_execution_id_sequencing(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_continue_as_new_creates_new_execution() {
        test_continue_as_new_creates_new_execution(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_execution_history_persistence() {
        test_execution_history_persistence(&TursoTestFactory).await;
    }

    // Queue semantics tests
    #[tokio::test]
    async fn test_turso_worker_queue_fifo_ordering() {
        test_worker_queue_fifo_ordering(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_worker_peek_lock_semantics() {
        test_worker_peek_lock_semantics(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_worker_ack_atomicity() {
        test_worker_ack_atomicity(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_timer_delayed_visibility() {
        test_timer_delayed_visibility(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_lost_lock_token_handling() {
        test_lost_lock_token_handling(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_worker_item_immediate_visibility() {
        test_worker_item_immediate_visibility(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_worker_delayed_visibility_skips_future_items() {
        test_worker_delayed_visibility_skips_future_items(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_orphan_queue_messages_dropped() {
        test_orphan_queue_messages_dropped(&TursoTestFactory).await;
    }

    // Management tests
    #[tokio::test]
    async fn test_turso_list_instances() {
        test_list_instances(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_list_instances_by_status() {
        test_list_instances_by_status(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_list_executions() {
        test_list_executions(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_get_instance_info() {
        test_get_instance_info(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_get_execution_info() {
        test_get_execution_info(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_get_system_metrics() {
        test_get_system_metrics(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_get_queue_depths() {
        test_get_queue_depths(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_get_instance_stats_nonexistent() {
        test_get_instance_stats_nonexistent(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_get_instance_stats_history() {
        test_get_instance_stats_history(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_get_instance_stats_kv() {
        test_get_instance_stats_kv(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_get_instance_stats_carry_forward() {
        test_get_instance_stats_carry_forward(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_get_instance_stats_kv_delta_only() {
        test_get_instance_stats_kv_delta_only(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_get_instance_stats_kv_merged() {
        test_get_instance_stats_kv_merged(&TursoTestFactory).await;
    }

    // Instance creation tests
    #[tokio::test]
    async fn test_turso_instance_creation_via_metadata() {
        test_instance_creation_via_metadata(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_no_instance_creation_on_enqueue() {
        test_no_instance_creation_on_enqueue(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_null_version_handling() {
        test_null_version_handling(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_sub_orchestration_instance_creation() {
        test_sub_orchestration_instance_creation(&TursoTestFactory).await;
    }

    // Long polling tests (Turso uses short polling)
    #[tokio::test]
    async fn test_turso_short_poll_returns_immediately() {
        let provider = TursoTestFactory.create_provider().await;
        test_short_poll_returns_immediately(&*provider, TursoTestFactory.short_poll_threshold()).await;
    }

    #[tokio::test]
    async fn test_turso_short_poll_work_item_returns_immediately() {
        let provider = TursoTestFactory.create_provider().await;
        test_short_poll_work_item_returns_immediately(&*provider, TursoTestFactory.short_poll_threshold()).await;
    }

    #[tokio::test]
    async fn test_turso_fetch_respects_timeout_upper_bound() {
        let provider = TursoTestFactory.create_provider().await;
        test_fetch_respects_timeout_upper_bound(&*provider).await;
    }

    // Poison message tests
    #[tokio::test]
    async fn test_turso_orchestration_attempt_count_starts_at_one() {
        orchestration_attempt_count_starts_at_one(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_orchestration_attempt_count_increments_on_refetch() {
        orchestration_attempt_count_increments_on_refetch(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_worker_attempt_count_starts_at_one() {
        worker_attempt_count_starts_at_one(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_worker_attempt_count_increments_on_lock_expiry() {
        worker_attempt_count_increments_on_lock_expiry(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_attempt_count_is_per_message() {
        attempt_count_is_per_message(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_abandon_work_item_ignore_attempt_decrements() {
        abandon_work_item_ignore_attempt_decrements(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_abandon_orchestration_item_ignore_attempt_decrements() {
        abandon_orchestration_item_ignore_attempt_decrements(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_ignore_attempt_never_goes_negative() {
        ignore_attempt_never_goes_negative(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_max_attempt_count_across_message_batch() {
        max_attempt_count_across_message_batch(&TursoTestFactory).await;
    }

    // abandon_work_item tests
    #[tokio::test]
    async fn test_turso_abandon_work_item_releases_lock() {
        test_abandon_work_item_releases_lock(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_abandon_work_item_with_delay() {
        test_abandon_work_item_with_delay(&TursoTestFactory).await;
    }

    // Cancellation tests (activity cancellation support)
    #[tokio::test]
    async fn test_turso_fetch_returns_running_state_for_active_orchestration() {
        test_fetch_returns_running_state_for_active_orchestration(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_fetch_returns_terminal_state_when_orchestration_completed() {
        test_fetch_returns_terminal_state_when_orchestration_completed(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_fetch_returns_terminal_state_when_orchestration_failed() {
        test_fetch_returns_terminal_state_when_orchestration_failed(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_fetch_returns_terminal_state_when_orchestration_continued_as_new() {
        test_fetch_returns_terminal_state_when_orchestration_continued_as_new(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_fetch_returns_missing_state_when_instance_deleted() {
        test_fetch_returns_missing_state_when_instance_deleted(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_renew_returns_running_when_orchestration_active() {
        test_renew_returns_running_when_orchestration_active(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_renew_returns_terminal_when_orchestration_completed() {
        test_renew_returns_terminal_when_orchestration_completed(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_renew_returns_missing_when_instance_deleted() {
        test_renew_returns_missing_when_instance_deleted(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_ack_work_item_none_deletes_without_enqueue() {
        test_ack_work_item_none_deletes_without_enqueue(&TursoTestFactory).await;
    }

    // Lock-stealing activity cancellation tests
    #[tokio::test]
    async fn test_turso_cancelled_activities_deleted_from_worker_queue() {
        test_cancelled_activities_deleted_from_worker_queue(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_ack_work_item_fails_when_entry_deleted() {
        test_ack_work_item_fails_when_entry_deleted(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_renew_fails_when_entry_deleted() {
        test_renew_fails_when_entry_deleted(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_cancelling_nonexistent_activities_is_idempotent() {
        test_cancelling_nonexistent_activities_is_idempotent(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_batch_cancellation_deletes_multiple_activities() {
        test_batch_cancellation_deletes_multiple_activities(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_same_activity_in_worker_items_and_cancelled_is_noop() {
        test_same_activity_in_worker_items_and_cancelled_is_noop(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_orphan_activity_after_instance_force_deletion() {
        test_orphan_activity_after_instance_force_deletion(&TursoTestFactory).await;
    }

    // Deletion tests
    #[tokio::test]
    async fn test_turso_delete_terminal_instances() {
        test_delete_terminal_instances(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_delete_running_rejected_force_succeeds() {
        test_delete_running_rejected_force_succeeds(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_delete_nonexistent_instance() {
        test_delete_nonexistent_instance(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_delete_cleans_queues_and_locks() {
        test_delete_cleans_queues_and_locks(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_cascade_delete_hierarchy() {
        test_cascade_delete_hierarchy(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_force_delete_prevents_ack_recreation() {
        test_force_delete_prevents_ack_recreation(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_list_children() {
        test_list_children(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_delete_get_parent_id() {
        test_delete_get_parent_id(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_delete_get_instance_tree() {
        test_delete_get_instance_tree(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_delete_instances_atomic() {
        test_delete_instances_atomic(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_delete_instances_atomic_force() {
        test_delete_instances_atomic_force(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_delete_instances_atomic_orphan_detection() {
        test_delete_instances_atomic_orphan_detection(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_stale_activity_after_delete_recreate() {
        test_stale_activity_after_delete_recreate(&TursoTestFactory).await;
    }

    // Worker lock renewal tests
    #[tokio::test]
    async fn test_turso_worker_lock_renewal_success() {
        test_worker_lock_renewal_success(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_worker_lock_renewal_invalid_token() {
        test_worker_lock_renewal_invalid_token(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_worker_lock_renewal_after_expiration() {
        test_worker_lock_renewal_after_expiration(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_worker_lock_renewal_extends_timeout() {
        test_worker_lock_renewal_extends_timeout(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_worker_lock_renewal_after_ack() {
        test_worker_lock_renewal_after_ack(&TursoTestFactory).await;
    }

    // Lock expiry boundary tests
    #[tokio::test]
    async fn test_turso_worker_ack_fails_after_lock_expiry() {
        test_worker_ack_fails_after_lock_expiry(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_orchestration_lock_renewal_after_expiration() {
        test_orchestration_lock_renewal_after_expiration(&TursoTestFactory).await;
    }

    // Prune tests
    #[tokio::test]
    async fn test_turso_prune_options_combinations() {
        test_prune_options_combinations(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_prune_safety() {
        test_prune_safety(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_prune_bulk() {
        test_prune_bulk(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_prune_bulk_includes_running_instances() {
        test_prune_bulk_includes_running_instances(&TursoTestFactory).await;
    }

    // Bulk deletion tests
    #[tokio::test]
    async fn test_turso_delete_instance_bulk_filter_combinations() {
        test_delete_instance_bulk_filter_combinations(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_delete_instance_bulk_safety_and_limits() {
        test_delete_instance_bulk_safety_and_limits(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_delete_instance_bulk_completed_before_filter() {
        test_delete_instance_bulk_completed_before_filter(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_delete_instance_bulk_cascades_to_children() {
        test_delete_instance_bulk_cascades_to_children(&TursoTestFactory).await;
    }

    // Capability filtering tests
    #[tokio::test]
    async fn test_turso_fetch_with_filter_none_returns_any_item() {
        test_fetch_with_filter_none_returns_any_item(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_fetch_with_compatible_filter_returns_item() {
        test_fetch_with_compatible_filter_returns_item(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_fetch_with_incompatible_filter_skips_item() {
        test_fetch_with_incompatible_filter_skips_item(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_fetch_filter_skips_incompatible_selects_compatible() {
        test_fetch_filter_skips_incompatible_selects_compatible(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_fetch_filter_does_not_lock_skipped_instances() {
        test_fetch_filter_does_not_lock_skipped_instances(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_fetch_filter_null_pinned_version_always_compatible() {
        test_fetch_filter_null_pinned_version_always_compatible(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_fetch_filter_boundary_versions() {
        test_fetch_filter_boundary_versions(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_pinned_version_stored_via_ack_metadata() {
        test_pinned_version_stored_via_ack_metadata(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_pinned_version_immutable_across_ack_cycles() {
        test_pinned_version_immutable_across_ack_cycles(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_continue_as_new_execution_gets_own_pinned_version() {
        test_continue_as_new_execution_gets_own_pinned_version(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_filter_with_empty_supported_versions_returns_nothing() {
        test_filter_with_empty_supported_versions_returns_nothing(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_concurrent_filtered_fetch_no_double_lock() {
        test_concurrent_filtered_fetch_no_double_lock(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_ack_stores_pinned_version_via_metadata_update() {
        test_ack_stores_pinned_version_via_metadata_update(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_provider_updates_pinned_version_when_told() {
        test_provider_updates_pinned_version_when_told(&TursoTestFactory).await;
    }

    // Category I: Deserialization contract tests (provider-agnostic via ProviderFactory)
    // Category F2: Additional edge cases
    #[tokio::test]
    async fn test_turso_fetch_single_range_only_uses_first_range() {
        test_fetch_single_range_only_uses_first_range(&TursoTestFactory).await;
    }

    // ======================================================================
    // Session Routing Validations
    // ======================================================================

    #[tokio::test]
    async fn test_turso_non_session_items_fetchable_by_any_worker() {
        test_non_session_items_fetchable_by_any_worker(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_session_item_claimable_when_no_session() {
        test_session_item_claimable_when_no_session(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_session_affinity_same_worker() {
        test_session_affinity_same_worker(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_session_affinity_blocks_other_worker() {
        test_session_affinity_blocks_other_worker(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_different_sessions_different_workers() {
        test_different_sessions_different_workers(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_mixed_session_and_non_session_items() {
        test_mixed_session_and_non_session_items(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_session_claimable_after_lock_expiry() {
        test_session_claimable_after_lock_expiry(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_none_session_skips_session_items() {
        test_none_session_skips_session_items(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_some_session_returns_all_items() {
        test_some_session_returns_all_items(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_renew_session_lock_active() {
        test_renew_session_lock_active(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_renew_session_lock_skips_idle() {
        test_renew_session_lock_skips_idle(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_renew_session_lock_no_sessions() {
        test_renew_session_lock_no_sessions(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_cleanup_removes_expired_no_items() {
        test_cleanup_removes_expired_no_items(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_cleanup_keeps_sessions_with_pending_items() {
        test_cleanup_keeps_sessions_with_pending_items(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_cleanup_keeps_active_sessions() {
        test_cleanup_keeps_active_sessions(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_ack_updates_session_last_activity() {
        test_ack_updates_session_last_activity(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_renew_work_item_updates_session_last_activity() {
        test_renew_work_item_updates_session_last_activity(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_session_items_processed_in_order() {
        test_session_items_processed_in_order(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_non_session_items_returned_with_session_config() {
        test_non_session_items_returned_with_session_config(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_shared_worker_id_any_caller_can_fetch_owned_session() {
        test_shared_worker_id_any_caller_can_fetch_owned_session(&TursoTestFactory).await;
    }

    // ======================================================================
    // Session Race Condition Validations
    // ======================================================================

    #[tokio::test]
    async fn test_turso_concurrent_session_claim_only_one_wins() {
        test_concurrent_session_claim_only_one_wins(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_session_takeover_after_lock_expiry() {
        test_session_takeover_after_lock_expiry(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_cleanup_then_new_item_recreates_session() {
        test_cleanup_then_new_item_recreates_session(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_abandoned_session_item_retryable() {
        test_abandoned_session_item_retryable(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_abandoned_session_item_ignore_attempt() {
        test_abandoned_session_item_ignore_attempt(&TursoTestFactory).await;
    }

    // ======================================================================
    // Session Lock Expiry Boundary Validations
    // ======================================================================

    #[tokio::test]
    async fn test_turso_renew_session_lock_after_expiry_returns_zero() {
        test_renew_session_lock_after_expiry_returns_zero(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_original_worker_reclaims_expired_session() {
        test_original_worker_reclaims_expired_session(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_activity_lock_expires_session_lock_valid_same_worker_refetches() {
        test_activity_lock_expires_session_lock_valid_same_worker_refetches(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_both_locks_expire_different_worker_claims() {
        test_both_locks_expire_different_worker_claims(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_session_lock_expires_activity_lock_valid_ack_succeeds() {
        test_session_lock_expires_activity_lock_valid_ack_succeeds(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_session_lock_expires_new_owner_gets_redelivery() {
        test_session_lock_expires_new_owner_gets_redelivery(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_session_lock_expires_same_worker_reacquires() {
        test_session_lock_expires_same_worker_reacquires(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_session_lock_renewal_extends_past_original_timeout() {
        test_session_lock_renewal_extends_past_original_timeout(&TursoTestFactory).await;
    }

    // Custom status tests
    use duroxide::provider_validations::custom_status::{
        test_custom_status_clear, test_custom_status_default_on_new_instance, test_custom_status_none_preserves,
        test_custom_status_nonexistent_instance, test_custom_status_polling_no_change, test_custom_status_set,
        test_custom_status_version_increments,
    };

    #[tokio::test]
    async fn test_turso_custom_status_set() {
        test_custom_status_set(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_custom_status_clear() {
        test_custom_status_clear(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_custom_status_none_preserves() {
        test_custom_status_none_preserves(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_custom_status_version_increments() {
        test_custom_status_version_increments(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_custom_status_polling_no_change() {
        test_custom_status_polling_no_change(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_custom_status_nonexistent_instance() {
        test_custom_status_nonexistent_instance(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_custom_status_default_on_new_instance() {
        test_custom_status_default_on_new_instance(&TursoTestFactory).await;
    }

    // Tag filtering tests
    #[tokio::test]
    async fn test_turso_tag_default_only_fetches_untagged() {
        test_default_only_fetches_untagged(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_tag_tags_fetches_only_matching() {
        test_tags_fetches_only_matching(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_tag_default_and_fetches_untagged_and_matching() {
        test_default_and_fetches_untagged_and_matching(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_tag_none_filter_returns_nothing() {
        test_none_filter_returns_nothing(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_tag_multi_tag_filter() {
        test_multi_tag_filter(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_tag_round_trip_preservation() {
        test_tag_round_trip_preservation(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_tag_any_filter_fetches_everything() {
        test_any_filter_fetches_everything(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_tag_survives_abandon_and_refetch() {
        test_tag_survives_abandon_and_refetch(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_tag_multi_runtime_isolation() {
        test_multi_runtime_tag_isolation(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_tag_preserved_through_ack_orchestration_item() {
        test_tag_preserved_through_ack_orchestration_item(&TursoTestFactory).await;
    }

    // KV store tests
    use duroxide::provider_validations::kv_store::{
        test_kv_clear_all, test_kv_clear_isolation, test_kv_clear_nonexistent_key, test_kv_clear_single,
        test_kv_cross_execution_overwrite, test_kv_cross_execution_remove_readd, test_kv_delete_instance_cascades,
        test_kv_delete_instance_with_children, test_kv_empty_value, test_kv_execution_id_tracking,
        test_kv_get_nonexistent, test_kv_get_unknown_instance, test_kv_instance_isolation, test_kv_large_value,
        test_kv_overwrite, test_kv_prune_current_execution_protected, test_kv_prune_preserves_all_keys,
        test_kv_prune_preserves_overwritten, test_kv_set_after_clear, test_kv_set_and_get,
        test_kv_snapshot_after_clear_all, test_kv_snapshot_after_clear_single, test_kv_snapshot_cross_execution,
        test_kv_snapshot_empty, test_kv_snapshot_in_fetch, test_kv_special_chars_in_key,
    };

    #[tokio::test]
    async fn test_turso_kv_set_and_get() {
        test_kv_set_and_get(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_overwrite() {
        test_kv_overwrite(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_clear_single() {
        test_kv_clear_single(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_clear_all() {
        test_kv_clear_all(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_get_nonexistent() {
        test_kv_get_nonexistent(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_snapshot_in_fetch() {
        test_kv_snapshot_in_fetch(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_snapshot_after_clear_single() {
        test_kv_snapshot_after_clear_single(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_snapshot_after_clear_all() {
        test_kv_snapshot_after_clear_all(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_execution_id_tracking() {
        test_kv_execution_id_tracking(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_cross_execution_overwrite() {
        test_kv_cross_execution_overwrite(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_cross_execution_remove_readd() {
        test_kv_cross_execution_remove_readd(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_prune_preserves_overwritten() {
        test_kv_prune_preserves_overwritten(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_prune_preserves_all_keys() {
        test_kv_prune_preserves_all_keys(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_instance_isolation() {
        test_kv_instance_isolation(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_delete_instance_cascades() {
        test_kv_delete_instance_cascades(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_clear_nonexistent_key() {
        test_kv_clear_nonexistent_key(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_get_unknown_instance() {
        test_kv_get_unknown_instance(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_set_after_clear() {
        test_kv_set_after_clear(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_empty_value() {
        test_kv_empty_value(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_large_value() {
        test_kv_large_value(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_special_chars_in_key() {
        test_kv_special_chars_in_key(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_snapshot_empty() {
        test_kv_snapshot_empty(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_snapshot_cross_execution() {
        test_kv_snapshot_cross_execution(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_prune_current_execution_protected() {
        test_kv_prune_current_execution_protected(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_delete_instance_with_children() {
        test_kv_delete_instance_with_children(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_clear_isolation() {
        test_kv_clear_isolation(&TursoTestFactory).await;
    }

    // KV delta tests (spec for kv_delta change)
    use duroxide::provider_validations::kv_store::{
        test_kv_delta_clear_all_tombstones_store, test_kv_delta_client_reads_merged,
        test_kv_delta_delete_instance_cascades, test_kv_delta_merged_on_can, test_kv_delta_merged_on_completion,
        test_kv_delta_prune_untouched_key_survives, test_kv_delta_snapshot_excludes_current_execution,
        test_kv_delta_snapshot_includes_completed_execution, test_kv_delta_tombstone_overrides_store,
    };

    #[tokio::test]
    async fn test_turso_kv_delta_snapshot_excludes_current_execution() {
        test_kv_delta_snapshot_excludes_current_execution(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_delta_snapshot_includes_completed_execution() {
        test_kv_delta_snapshot_includes_completed_execution(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_delta_client_reads_merged() {
        test_kv_delta_client_reads_merged(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_delta_tombstone_overrides_store() {
        test_kv_delta_tombstone_overrides_store(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_delta_clear_all_tombstones_store() {
        test_kv_delta_clear_all_tombstones_store(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_delta_merged_on_completion() {
        test_kv_delta_merged_on_completion(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_delta_merged_on_can() {
        test_kv_delta_merged_on_can(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_delta_delete_instance_cascades() {
        test_kv_delta_delete_instance_cascades(&TursoTestFactory).await;
    }

    #[tokio::test]
    async fn test_turso_kv_delta_prune_untouched_key_survives() {
        test_kv_delta_prune_untouched_key_survives(&TursoTestFactory).await;
    }
    #[tokio::test]
    async fn test_turso_file_backed_persistence_with_turso_url() {
        let tempdir = tempfile::tempdir().unwrap();
        let db_path = tempdir.path().join("duroxide-turso.db");
        let database_url = format!("turso:{}", db_path.display());
        let instance = "turso-file-backed-instance";
        let orchestration = "FileBacked";
        let version = "1.0.0";
        let input = r#"{"persisted":true}"#;

        {
            let store = TursoProvider::new(&database_url, None).await.unwrap();
            store
                .enqueue_for_orchestrator(
                    WorkItem::StartOrchestration {
                        instance: instance.to_string(),
                        orchestration: orchestration.to_string(),
                        version: Some(version.to_string()),
                        input: input.to_string(),
                        parent_instance: None,
                        parent_id: None,
                        execution_id: INITIAL_EXECUTION_ID,
                    },
                    None,
                )
                .await
                .unwrap();

            let (_item, lock_token, _attempt_count) = store
                .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO, None)
                .await
                .unwrap()
                .expect("queued orchestration should be available");

            store
                .ack_orchestration_item(
                    &lock_token,
                    INITIAL_EXECUTION_ID,
                    vec![Event::with_event_id(
                        INITIAL_EVENT_ID,
                        instance,
                        INITIAL_EXECUTION_ID,
                        None,
                        EventKind::OrchestrationStarted {
                            name: orchestration.to_string(),
                            version: version.to_string(),
                            input: input.to_string(),
                            parent_instance: None,
                            parent_id: None,
                            carry_forward_events: None,
                            initial_custom_status: None,
                        },
                    )],
                    vec![],
                    vec![],
                    ExecutionMetadata {
                        orchestration_name: Some(orchestration.to_string()),
                        orchestration_version: Some(version.to_string()),
                        ..Default::default()
                    },
                    vec![],
                )
                .await
                .unwrap();

            store.checkpoint().await.unwrap();
        }

        let reopened = TursoProvider::new(&database_url, None).await.unwrap();
        let history = ProviderAdmin::read_history(&reopened, instance).await.unwrap();

        assert_eq!(
            ProviderAdmin::latest_execution_id(&reopened, instance).await.unwrap(),
            INITIAL_EXECUTION_ID
        );
        assert_eq!(history.len(), 1);
        assert!(matches!(
            &history[0].kind,
            EventKind::OrchestrationStarted {
                name,
                version: persisted_version,
                input: persisted_input,
                ..
            } if name == orchestration && persisted_version == version && persisted_input == input
        ));
    }
}
