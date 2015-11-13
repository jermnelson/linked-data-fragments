--[[
add_get_triple

This script is licensed under the GNU Affero version 3. Copyrighted
2015 by Jeremy Nelson <jermnelson@gmail.com>--]]
local function add(digest, value)  
  --[[if not redis.pcall("exists", digest) then 
     redis.pcall('set', digest, value)      
  end--]]
  redis.pcall('set', digest, value)
end

local function add_triple(triple_key)
  local triple_body = nil
  if KEYS[4] then
     triple_body = KEYS[4]
  end
  if triple_body == nil then 
    triple_body = '[{"@id": "'..KEYS[1]..'",'
    if KEYS[2] == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' then
      triple_body = triple_body..'"@type": ["'..KEYS[3]..'"]}]'
    else
      triple_body = triple_body..'"'..KEYS[2]..'":[{'
    end
    if string.match(KEYS[3], "^http") then
      triple_body = triple_body..'"@id": "'
    else
      triple_body = triple_body..'"@value": "'
    end
    triple_body = triple_body..KEYS[3]..'"}]}]'
  end
  redis.pcall('set', triple_key, triple_body)
end

local function add_hash(subject_digest, predicate_digest, object_digest)
  local subject_key = subject_digest..":pred-obj"
  redis.pcall('hset', subject_key, predicate_digest..":"..object_digest, 1)
  local predicate_key = predicate_digest..":subj-obj"
  redis.pcall('hset', predicate_key, subject_digest..":"..object_digest, 1)
  local object_key = object_digest..":subj-pred"
  redis.pcall('hset', object_key, subject_digest..":"..predicate_digest, 1)
end

local function add_set(subject_digest, predicate_digest, object_digest)
  local subject_key = subject_digest..":pred-obj"
  redis.pcall('sadd', subject_key, predicate_digest..":"..object_digest)
  local predicate_key = predicate_digest..":subj-obj"
  redis.pcall('sadd', predicate_key, subject_digest..":"..object_digest)
  local object_key = object_digest..":subj-pred"
  redis.pcall('sadd', object_key, subject_digest..":"..predicate_digest)
end

local subject_sha1 = redis.sha1hex(KEYS[1])
add(subject_sha1, KEYS[1])
local predicate_sha1 = redis.sha1hex(KEYS[2])
add(predicate_sha1, KEYS[2])
local object_sha1 = redis.sha1hex(KEYS[3])
add(object_sha1, KEYS[3])
if KEYS[4] then
  if KEYS[4] == "hash" then
    add_hash(subject_sha1, predicate_sha1, object_sha1)
  elseif KEYS[4] == "set" then
    add_set(subject_sha1, predicate_sha1, object_sha1)
  else
    add_string(subject_sha1, predicate_sha1, object_sha1)
  end
else
  add_set(subject_sha1, predicate_sha1, object_sha1)
end
return true
--[[
local key =  subject_sha1..":"..predicate_sha1..":"..object_sha1
if redis.pcall("exists", key) then
  return key, redis.pcall("get", key)
else
  [add_triple(key)
 redis.pcall("set", key, 1)
end
return key
--]]
