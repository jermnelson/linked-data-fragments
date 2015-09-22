--[[
get_triple_search

This script is licensed under the GNU Affero version 3. Copyrighted
2015 by Jeremy Nelson <jermnelson@gmail.com>
--]]
local subject_sha1, predicate_sha1, object_sha1 = split(KEYS[1], ":")
local output = '[{"@id": "'
output = output..redis.pcall('get', subject_sha1_)..'",'
output = output..redis.pcall('get', predicate_sha1)..'":[{'
local object = redis.pcall('get', object_sha1)
if string.sub(object,1,string.len("http")) == 'http' then
    output = output..'"@id": "'
else
   output =  output..'"@value": "'
end
output = output..'"'..object..'"}]}]'
return output
