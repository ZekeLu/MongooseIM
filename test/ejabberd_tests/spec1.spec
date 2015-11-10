%% Spec examples:
%%
%%   {suites, "tests", amp_SUITE}.
%%   {groups, "tests", amp_SUITE, [discovery]}.
%%   {groups, "tests", amp_SUITE, [discovery], {cases, [stream_feature_test]}}.
%%   {cases, "tests", amp_SUITE, [stream_feature_test]}.
%%
%% For more info see:
%% http://www.erlang.org/doc/apps/common_test/run_test_chapter.html#test_specifications


%% Add by sharp.
%% new test add a ct_hook 'ct_mongoose_hook', this hook will show many logs like:
%% "Suite: adhoc_SUITE finished dirty. Other suites may fail because of that. Details:
%%  [{registered_users_count,2}]"
%% ct_mongoose_hook rpc 'ejabberd_auth:do_get_vh_registered_users_number/1' function,
%% this function use "select table_rows from information_schema.tables where table_name='users'"
%% to get uses number after removed, as 'select table_rows' is a way to estimate table rows,
%% it is not accurate when table rows is few.



%% do not remove below SUITE if testing mongoose
{suites, "tests", mongoose_sanity_checks_SUITE}.

{suites, "tests", adhoc_SUITE}.
{suites, "tests", amp_SUITE}.
{suites, "tests", carboncopy_SUITE}.
{suites, "tests", cluster_commands_SUITE}.
%% conf_reload_SUITE test change confile domain, ejabberd restart some module attach domain, but will stop 'ejabberd_auth_anonyous',
%% this will cause error, as we delete this module, so comment this suit.
%%{suites, "tests", conf_reload_SUITE}.
 %% need pem file.
%%{suites, "tests", connect_SUITE}.
{suites, "tests", ejabberdctl_SUITE}.
{suites, "tests", last_SUITE}.
{suites, "tests", login_SUITE}.
%% the BOSH client will send this request to the server when the connection is closing even after the user is unregistered.
%%   <body type='terminate' sid='sid1' xmlns='http://jabber.org/protocol/httpbind' rid='rid1'>
%%     <presence type='unavailable'/>
%%   </body>
%% when the server receive this request, it will upsert the "last" table.
%% in some cases, the upsertion will happen after the deletion so the record for the unregistered user will be left in the table
%% which will affect other tests. So we move the bost_SUITE to the last
{suites, "tests", bosh_SUITE}.
{config, ["test.config"]}.
{logdir, "ct_report"}.
{ct_hooks, [ct_tty_hook, ct_mongoose_hook]}.
