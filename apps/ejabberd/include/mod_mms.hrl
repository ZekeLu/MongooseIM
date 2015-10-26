%%%-------------------------------------------------------------------
%%% @doc mms module
%%%
%%%-------------------------------------------------------------------

%% file type
-define(AVATAR, <<"1">>).
-define(MESSAGE, <<"2">>).
-define(PROJECT, <<"3">>).

-define(ENV(X), mod_mms_s3:get_env(X)).
-define(S3_CONFIG, mod_mms_s3:config()).
-define(MMS_SECRET, mod_mms_s3:secret()).

%% ==================
%% mms records
%% ==================

-record(mms_file, {
    id :: binary(),
    filename :: binary(),
    owner :: binary(),
    uid :: binary(),
    type :: binary(),
    created_at :: integer()
}).