--[[
add_get_hash

This script is licensed under the GNU Affero version 3. Copyrighted
2015 by Jeremy Nelson <jermnelson@gmail.com>

add_get_hash.lua should be loaded into your Redis cache server. 
The sha1 digest of the entity will be returned to calling
client when this function's sha1 digest called with a
single KEYS key.
--]]
local namespace = nil
local value = nil
local expires = 604800
--[[namespace, value = string.find(KEYS[1], "^(%w+):(%w+)")
if not namespace then
  local namespace_url = redis.pcall("hget", "namespaces", namespace)
  if namespace_url then
    value = namespace_url[0]..value
  else
    redis.pcall("hset", "namespace", namespace, nil)
  end
end--]]
if not value then
  value = KEYS[1]
end  
local digest = redis.sha1hex(value)
redis.call("set", digest, value)
--[[if not redis.call("exists", digest) then
  redis.call("set", digest, value)
end--]]
if ARGV[1] then
  expires = ARGV[1]
end
--[[redis.pcall("expires", digest, expires)--]]
return digest
