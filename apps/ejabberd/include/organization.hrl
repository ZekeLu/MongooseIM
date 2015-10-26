%% ==============================
%% organization
%%
%% ===============================

-record(group, {
    groupid :: binary() | integer(),
    groupname :: binary(),
    master :: binary()|ejabberd:jid(),
    type :: binary() | integer(),
    status :: binary() | integer(),
    project :: binary() | integer(),
    avatar :: binary(),
    private :: binary()
}).

-record(node, {
    id :: integer() | binary()|undefined,
    name :: binary(),
    lft :: binary() | integer(),
    rgt :: binary() | integer(),
    depth :: binary() | integer(),
    department :: binary(),
    project :: binary() | integer()
}).

-record(employee, {
    jid :: binary(),
    organization_id :: integer(),
    organization_name :: binary()
}).

-record(project, {
    id :: integer()|binary()|undefined,
    name :: binary(),
    description :: binary(),
    photo :: binary(),
    status :: integer(),
    admin :: binary(),
    start_at :: binary(),
    end_at :: binary()|undefined,
    job_tag :: binary(),
    member_tag :: binary(),
    link_tag :: binary()
}).

%% define group type
-define(NORAML_GROUP, <<"1">>).
-define(TASK_GROUP, <<"2">>).
-define(EVENT_GROUP, <<"3">>).
-define(FILE_GROUP, <<"4">>).

%% define group status
-define(STATUS_START, <<"1">>).
-define(STATUS_END, <<"2">>).
