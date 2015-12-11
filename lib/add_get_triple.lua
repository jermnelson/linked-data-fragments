--[[
add_get_triple

This script is licensed under the GNU Affero version 3. 
Copyrighted 2015 by Jeremy Nelson <jermnelson@gmail.com>
--]]
local function add(value) 
  local digest = redis.sha1hex(value) 
  redis.pcall('setnx', digest, value)
  return digest
end

local function add_string(subject_digest, predicate_digest, object_digest)
  local triple_key = subject_digest..":"..predicate_digest..":"..object_digest
  local triple_body = 1
  if ARGV[1] then
     triple_body = ARGV[1]
  end
  redis.pcall('setnx', triple_key, triple_body)
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

local subject_sha1 = add(KEYS[1])
local predicate_sha1 = add(KEYS[2])
local object_sha1 = add(KEYS[3])
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
