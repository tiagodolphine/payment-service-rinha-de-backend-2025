local function push_data(premature)
            # need to specify the resolver to resolve the hostname
            resolver 8.8.8.8;

            content_by_lua_block {
                local http = require "resty.http.fast"
                local httpc = http.new()
                httpc:connect{
                    scheme = "http",
                    host = "127.0.0.1",
                    port = ngx.var.server_port
                }

                local res, err = httpc:request{
                    path = "/b"
                }

                ngx.status = res.status
                ngx.print(res:read_body())

                httpc:close()
            }

   end

--Calling endpoint
ngx.timer.at(0, push_data)