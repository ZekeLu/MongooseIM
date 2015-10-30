%% Spec examples:
%%
%%   {suites, "tests", amp_SUITE}.
%%   {groups, "tests", amp_SUITE, [discovery]}.
%%   {groups, "tests", amp_SUITE, [discovery], {cases, [stream_feature_test]}}.
%%   {cases, "tests", amp_SUITE, [stream_feature_test]}.
%%
%% For more info see:
%% http://www.erlang.org/doc/apps/common_test/run_test_chapter.html#test_specifications

%% do not remove below SUITE if testing mongoose
{suites, "tests", mongoose_sanity_checks_SUITE}.

%% mam_SUITE, configurations: odbc_mnesia, odbc_async_cache, odbc_cache
{groups, "tests", mam_SUITE, [
    odbc_mnesia_mam,
    odbc_mnesia_mam_purge,
    odbc_mnesia_muc,
    odbc_mnesia_muc_with_pm,
    odbc_mnesia_rsm,
    odbc_mnesia_with_rsm,
    odbc_mnesia_muc_rsm,
    odbc_mnesia_bootstrapped,
    odbc_mnesia_archived,
    odbc_mnesia_policy_violation,
    odbc_mnesia_offline_message,
    odbc_async_cache_mam,
    odbc_async_cache_mam_purge,
    odbc_async_cache_muc,
    odbc_async_cache_muc_with_pm,
    odbc_async_cache_rsm,
    odbc_async_cache_with_rsm,
    odbc_async_cache_muc_rsm,
    odbc_async_cache_bootstrapped,
    odbc_async_cache_archived,
    odbc_async_cache_policy_violation,
    odbc_async_cache_offline_message,
    odbc_cache_mam,
    odbc_cache_mam_purge,
    odbc_cache_muc,
    odbc_cache_muc_with_pm,
    odbc_cache_rsm,
    odbc_cache_with_rsm,
    odbc_cache_muc_rsm,
    odbc_cache_bootstrapped,
    odbc_cache_archived,
    odbc_cache_policy_violation,
    odbc_cache_offline_message
]}.
{config, ["test.config"]}.
{logdir, "ct_report"}.
{ct_hooks, [ct_tty_hook, ct_mongoose_hook]}.
