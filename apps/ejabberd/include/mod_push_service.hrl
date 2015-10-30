%% ============================================
%% for mod_push_service
%% ============================================

-define(ODBC_SERVER, mod_push_service:get_host_server()).

-record(push_token, {
    jid :: binary(),
    token :: binary(),
    type :: integer()
}).

-record(push_message, {
    lang :: ejabberd:lang(),
    lserver :: binary(),
    fromuser :: binary(),
    fromjid :: binary(),
    touser :: binary(),
    tojid :: binary(),
    type :: text | audio| video | picture | location | undefined,
    groupid :: binary() | undefined,
    content :: binary() | undefined
}).