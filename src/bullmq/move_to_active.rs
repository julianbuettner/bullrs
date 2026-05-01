// The active-job hash returned by `moveToActive-11.lua` is a key/value array
// without a fixed schema (BullMQ adds fields over time). We continue to parse
// it field-by-field at the boundary in `luacommands::move_to_active::map_value`,
// rather than introducing a wire struct here.
