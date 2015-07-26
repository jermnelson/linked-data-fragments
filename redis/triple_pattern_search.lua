--[[
triple_pattern_search.

This script is licensed under the GNU Affero version 3. Copyrighted
2015 by Jeremy Nelson <jermnelson@gmail.com>
--]]
local output = {}
--[[if redis.pcall("exists", KEYS[1]) then
  local subject, predicate, object = string.match("^(%a+):(%a+):(%a+)")
  output
--]]  
local cursor = ARGV[1]
if not cursor then
  cursor = 0
end
cursor, result = redis.pcall("scan", "match="..KEYS[1]) 
return output

