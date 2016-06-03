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

-record(groupuser, {
    id :: binary() | integer() | undefined,
    groupid :: binary() | integer(),
    jid :: binary(),
    nickname :: binary()
}).

-record(node, {
    id :: integer() | binary()|undefined,
    name :: binary(),
    lft :: binary() | integer(),
    rgt :: binary() | integer(),
    depth :: binary() | integer(),
    department :: binary(),
    department_level :: binary(),
    department_id :: binary(),
    project :: binary() | integer()
}).

-record(employee, {
    jid :: binary(),
    organization_id :: integer(),
    organization_name :: binary()
}).

-record(project, {
    id :: integer()|binary()|undefined,
    name :: binary() | undefined,
    description :: binary() | undefined,
    photo :: binary() | undefined,
    status :: integer() | undefined,
    admin :: binary() | undefined,
    start_at :: binary() | undefined,
    end_at :: binary() | undefined,
    job_tag :: binary() | undefined,
    member_tag :: binary() | undefined,
    link_tag :: binary() | undefined,
    work_url :: binary() | undefined,
    city :: binary() | undefined,
    background :: binary() | undefined
}).

%% define group type
-define(NORAML_GROUP, <<"1">>).
-define(TASK_GROUP, <<"2">>).
-define(EVENT_GROUP, <<"3">>).
-define(FILE_GROUP, <<"4">>).

%% define group status
-define(STATUS_START, <<"1">>).
-define(STATUS_END, <<"2">>).
