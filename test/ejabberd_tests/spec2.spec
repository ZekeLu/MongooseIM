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

%% mam_SUITE, configurations: odbc_async_pool, odbc
{groups, "tests", mam_SUITE, [
    odbc_async_pool_mam,
    odbc_async_pool_mam_purge,
    odbc_async_pool_muc,
    odbc_async_pool_muc_with_pm,
    odbc_async_pool_rsm,
    odbc_async_pool_with_rsm,
    odbc_async_pool_muc_rsm,
    odbc_async_pool_bootstrapped,
    odbc_async_pool_archived,
    odbc_async_pool_policy_violation,
    odbc_async_pool_offline_message,
    odbc_mam,
    odbc_mam_purge,
    odbc_muc,
    odbc_muc_with_pm,
    odbc_rsm,
    odbc_with_rsm,
    odbc_muc_rsm,
    odbc_bootstrapped,
    odbc_archived,
    odbc_policy_violation,
    odbc_offline_message
]}.
{config, ["test.config"]}.
{logdir, "ct_report"}.
{ct_hooks, [ct_tty_hook, ct_mongoose_hook]}.
