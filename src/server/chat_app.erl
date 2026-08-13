-module(chat_app).
-behaviour(application).

%% OTP application 入口，实际启动工作交给顶层监督者 chat_sup。

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    chat_sup:start_link().

stop(_State) ->
    ok.
