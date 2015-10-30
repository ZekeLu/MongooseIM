%%%-------------------------------------------------------------------
%%% for mod_file_library
%%%-------------------------------------------------------------------

-type bint() :: binary() | integer().

-record(folder, {
    id :: bint() | undefined,
    parent :: bint() | undefined,
    project :: bint(),
    name :: binary(),
    description :: binary(),
    created_by :: binary(),
    created_at :: binary()
}).

-record(file, {
    id :: bint() | undefined,
    uid :: binary(),
    folder :: bint(),
    name :: binary(),
    version :: bint(),
    uploaded_by :: binary(),
    uploaded_at :: binary()
}).


-record(permission, {
    id :: bint() | undefined,
    folder :: bint(),
    organization :: bint(),
    created_by :: binary(),
    created_at :: binary()
}).


-define(REQUEST_PARAM_ERROR, <<"14101">>).
-define(FOLDER_READ_DENY, <<"14102">>).
-define(FOLDER_WRITE_DENY, <<"14103">>).
-define(OBJECT_NOT_EXISTS, <<"14104">>).
-define(DUPLICATE_FOLDER_NAME, <<"14105">>).
