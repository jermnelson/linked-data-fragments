--[[
add_get_triple

This script is licensed under the GNU Affero version 3. Copyrighted
2015 by Jeremy Nelson <jermnelson@gmail.com>--]]
local function add(digest, value)  
  if not redis.pcall("exists", digest) then 
     redis.set(digest, value)      
  end
end

local subject_sha1 = redis.sha1hex(KEYS[1])
add(subject_sha1, KEYS[1])
local predicate_sha1 = redis.sha1hex(KEYS[2])
add(predicate_sha1, KEYS[2])
local object_sha1 = redis.sha1hex(KEYS[3])
add(object_sha1, KEYS[3])
local key =  subject_sha1..":"..predicate_sha1..":"..object_sha1
if redis.pcall("exists", key) then
    return key, redis.pcall("get", key)
end
return key
