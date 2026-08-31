local array = sink.v1.array
local object = sink.v1.object

return function(current, incoming)
    current = current or json.object()
    object.replace_nonempty_string(current, incoming, "title")
    current.tags = array.union_strings(current.tags, incoming.tags)
    current.updated_at = sink.v1.time.now()
    return current
end
