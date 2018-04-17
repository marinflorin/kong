local Errors      = require "kong.db.errors"
local responses   = require "kong.tools.responses"
local utils       = require "kong.tools.utils"
local app_helpers = require "lapis.application"


local escape_uri  = ngx.escape_uri
local null        = ngx.null
local fmt         = string.format
local sub         = string.sub


-- error codes http status codes
local ERRORS_HTTP_CODES = {
  [Errors.codes.INVALID_PRIMARY_KEY]   = 400,
  [Errors.codes.SCHEMA_VIOLATION]      = 400,
  [Errors.codes.PRIMARY_KEY_VIOLATION] = 400,
  [Errors.codes.FOREIGN_KEY_VIOLATION] = 400,
  [Errors.codes.UNIQUE_VIOLATION]      = 409,
  [Errors.codes.NOT_FOUND]             = 404,
  [Errors.codes.INVALID_OFFSET]        = 400,
  [Errors.codes.DATABASE_ERROR]        = 500,
}


local function handle_error(err_t)
  local status = ERRORS_HTTP_CODES[err_t.code]
  if not status or status == 500 then
    return app_helpers.yield_error(err_t)
  end

  responses.send(status, err_t)
end


local function extract_unique_field_names(schema)
  local unique_field_names = {}
  local len = 0

  for fname, field in schema:each_field() do
    if field.unique then
      len = len + 1
      unique_field_names[len] = fname
    end
  end

  return unique_field_names
end


local function select_by_param(dao, unique_field_names, param)
  -- TODO: composite key support
  if utils.is_valid_uuid(param) then
    return dao:select({ id = param })
  end

  for _, fname in ipairs(unique_field_names) do
    local row, err, err_t = dao["select_by_" .. fname](dao, param)
    if err_t then
      return nil, err, err_t
    end

    if row then
      return row
    end
  end
end


-- Generates admin api get collection endpoint functions
--
-- Examples:
--
-- /routes
-- /services/:services/routes
--
-- and
--
-- /services
local function get_collection_endpoint(schema_name, entity_name,
                                       parent_schema_name,
                                       parent_entity_unique_field_names)
  if not parent_schema_name then
    return function(self, db, helpers)
      local data, _, err_t, offset = db[schema_name]:page(self.args.size,
                                                          self.args.offset)
      if err_t then
        return handle_error(err_t)
      end

      local next_page = offset and fmt("/%s?offset=%s", schema_name,
                                       escape_uri(offset)) or null

      return helpers.responses.send_HTTP_OK {
        data   = data,
        offset = offset,
        next   = next_page,
      }
    end
  end

  return function(self, db, helpers)
    -- TODO: composite key support
    local parent_id = self.params[parent_schema_name]
    local parent_entity, _, err_t = select_by_param(db[parent_schema_name],
                                                    parent_entity_unique_field_names,
                                                    parent_id)
    if err_t then
      return handle_error(err_t)
    end

    if not parent_entity then
      return helpers.responses.send_HTTP_NOT_FOUND()
    end

    -- TODO: composite key support
    local dao = db[schema_name]
    local rows, _, err_t, offset = dao["for_" .. entity_name](dao, {
      id = parent_entity.id
    }, self.args.size, self.args.offset)
    if err_t then
      return handle_error(err_t)
    end

    local next_page = offset and fmt("/%s/%s/%s?offset=%s", parent_schema_name,
                                     escape_uri(parent_id), schema_name,
                                     escape_uri(offset)) or null

    return helpers.responses.send_HTTP_OK {
      data   = rows,
      offset = offset,
      next   = next_page,
    }
  end
end


-- Generates admin api post collection endpoint functions
--
-- Examples:
--
-- /routes
-- /services/:services/routes
--
-- and
--
-- /services
local function post_collection_endpoint(schema_name, entity_name,
                                        parent_schema_name,
                                        parent_entity_unique_field_names)
  return function(self, db, helpers)
    if parent_schema_name then
      local id = self.params[parent_schema_name]

      -- TODO: composite key support
      local parent_entity, _, err_t = select_by_param(db[parent_schema_name],
                                                      parent_entity_unique_field_names,
                                                      id)
      if err_t then
        return handle_error(err_t)
      end

      if not parent_entity then
        return helpers.responses.send_HTTP_NOT_FOUND()
      end

      -- TODO: composite key support
      self.args.post[entity_name] = { id = parent_entity.id }
    end

    local data, _, err_t = db[schema_name]:insert(self.args.post)
    if err_t then
      return handle_error(err_t)
    end

    return helpers.responses.send_HTTP_CREATED(data)
  end
end


-- Generates admin api get entity endpoint functions
--
-- Examples:
--
-- /routes/:routes
-- /routes/:routes/service
--
-- and
--
-- /services/:services
local function get_entity_endpoint(schema_name,
                                   entity_unique_field_names,
                                   entity_name,
                                   parent_schema_name,
                                   parent_entity_unique_field_names)
  return function(self, db, helpers)
    local entity, _, err_t

    if not parent_schema_name then
      -- TODO: composite key support
      local id = self.params[schema_name]
      entity, _, err_t = select_by_param(db[schema_name],
                                         entity_unique_field_names,
                                         id)
    else
      -- TODO: composite key support
      local parent_id = self.params[parent_schema_name]
      local parent_entity
      parent_entity, _, err_t = select_by_param(db[parent_schema_name],
                                                parent_entity_unique_field_names,
                                                parent_id)
      if err_t then
        return handle_error(err_t)
      end

      if not parent_entity or parent_entity[entity_name] == null then
        return helpers.responses.send_HTTP_NOT_FOUND()
      end

      entity, _, err_t = db[schema_name]:select(parent_entity[entity_name])
    end

    if err_t then
      return handle_error(err_t)
    end

    if not entity then
      return helpers.responses.send_HTTP_NOT_FOUND()
    end

    return helpers.responses.send_HTTP_OK(entity)
  end
end


-- Generates admin api patch entity endpoint functions
--
-- Examples:
--
-- /routes/:routes
-- /routes/:routes/service
--
-- and
--
-- /services/:services
local function patch_entity_endpoint(schema_name,
                                     entity_unique_field_names,
                                     entity_name,
                                     parent_schema_name,
                                     parent_entity_unique_field_names)
  return function(self, db, helpers)
    local entity, _, err_t

    if not parent_schema_name then
      local dao = db[schema_name]
      -- TODO: composite key support
      local id = self.params[schema_name]
      if utils.is_valid_uuid(id) then
        entity, _, err_t = dao:update({ id = id }, self.args.post)

      elseif #entity_unique_field_names == 1 then
        local fname = entity_unique_field_names[1]
        entity, _, err_t = dao["update_by_" .. fname](dao, id, self.args.post)

      else
        entity, _, err_t = select_by_param(dao,
                                           entity_unique_field_names,
                                           id)
        if err_t then
          return handle_error(err_t)
        end

        if not entity then
          return helpers.responses.send_HTTP_NOT_FOUND()
        end

        entity, _, err_t = dao:update({ id = entity.id }, self.args.post)
      end

    else
      -- TODO: composite key support
      local parent_id = self.params[parent_schema_name]
      local parent_entity
      parent_entity, _, err_t = select_by_param(db[parent_schema_name],
                                                parent_entity_unique_field_names,
                                                parent_id)

      if err_t then
        return handle_error(err_t)
      end

      if not parent_entity or parent_entity[entity_name] == null then
        return helpers.responses.send_HTTP_NOT_FOUND()
      end

      entity, _, err_t = db[schema_name]:update(parent_entity[entity_name],
                                                self.args.post)
    end

    if err_t then
      return handle_error(err_t)
    end

    return helpers.responses.send_HTTP_OK(entity)
  end
end


-- Generates admin api delete entity endpoint functions
--
-- Examples:
--
-- /routes/:routes
-- /routes/:routes/service
--
-- and
--
-- /services/:services
local function delete_entity_endpoint(schema_name,
                                      entity_unique_field_names,
                                      entity_name,
                                      parent_schema_name,
                                      parent_entity_unique_field_names)
  return function(self, db, helpers)
    local _, err_t
    if not parent_schema_name then
      local dao = db[schema_name]

      -- TODO: composite key support
      local id = self.params[schema_name]
      if utils.is_valid_uuid(id) then
        _, _, err_t = db[schema_name]:delete({ id = id })

      elseif #entity_unique_field_names == 1 then
        local fname = entity_unique_field_names[1]
        _, _, err_t = dao["delete_by_" .. fname](dao, id)

      else
        local entity
        entity, _, err_t = select_by_param(dao, entity_unique_field_names, id)

        if err_t then
          return handle_error(err_t)
        end

        if not entity then
          return helpers.responses.send_HTTP_NOT_FOUND()
        end

        _, _, err_t = dao:delete({ id = entity.id }, self.args.post)
      end

      if err_t then
        return handle_error(err_t)
      end

      return helpers.responses.send_HTTP_NO_CONTENT()

    else
      -- TODO: composite key support
      local parent_id = self.params[parent_schema_name]
      local parent_entity
      parent_entity, _, err_t = select_by_param(db[parent_schema_name],
                                                parent_entity_unique_field_names,
                                                parent_id)
      if err_t then
        return handle_error(err_t)
      end

      if not parent_entity or parent_entity[entity_name] == null then
        return helpers.responses.send_HTTP_NOT_FOUND()
      end

      return helpers.responses.send_HTTP_METHOD_NOT_ALLOWED()
    end
  end
end


local function generate_collection_endpoints(endpoints, collection_path, ...)
  endpoints[collection_path] = {
    --OPTIONS = method_not_allowed,
    --HEAD    = method_not_allowed,
    GET     = get_collection_endpoint(...),
    POST    = post_collection_endpoint(...),
    --PUT     = method_not_allowed,
    --PATCH   = method_not_allowed,
    --DELETE  = method_not_allowed,
  }
end


local function generate_entity_endpoints(endpoints, entity_path, ...)
  endpoints[entity_path] = {
    --OPTIONS = method_not_allowed,
    --HEAD    = method_not_allowed,
    GET     = get_entity_endpoint(...),
    --POST    = method_not_allowed,
    --PUT     = method_not_allowed,
    PATCH   = patch_entity_endpoint(...),
    DELETE  = delete_entity_endpoint(...),
  }
end





-- Generates admin api endpoint functions
--
-- Examples:
--
-- /routes
-- /routes/:routes
-- /routes/:routes/service
-- /services/:services/routes
--
-- and
--
-- /services
-- /services/:services
local function generate_endpoints(schema, endpoints, prefix)
  local path_prefix
  if prefix then
    if sub(prefix, -1) == "/" then
      path_prefix = prefix

    else
      path_prefix = prefix .. "/"
    end

  else
    path_prefix = "/"
  end

  local schema_name = schema.name
  local collection_path = path_prefix .. schema_name

  -- e.g. /routes
  generate_collection_endpoints(endpoints, collection_path, schema_name)

  local entity_path = fmt("%s/:%s", collection_path, schema_name)
  local entity_unique_field_names = extract_unique_field_names(schema)

  -- e.g. /routes/:routes
  generate_entity_endpoints(endpoints, entity_path, schema_name,
                            entity_unique_field_names)

  for foreign_field_name, foreign_field in schema:each_field() do
    if foreign_field.type == "foreign" then
      local foreign_schema      = foreign_field.schema
      local foreign_schema_name = foreign_schema.name

      local foreign_entity_path = fmt("%s/%s", entity_path, foreign_field_name)
      local foreign_entity_unique_field_names = extract_unique_field_names(foreign_schema)

      -- e.g. /routes/:routes/service
      generate_entity_endpoints(endpoints, foreign_entity_path,
                                foreign_schema_name,
                                foreign_entity_unique_field_names,
                                foreign_field_name, schema_name,
                                entity_unique_field_names)

      -- e.g. /services/:services/routes
      local foreign_collection_path = fmt("/%s/:%s/%s", foreign_schema_name,
                                          foreign_schema_name, schema_name)

      generate_collection_endpoints(endpoints, foreign_collection_path,
                                    schema_name, foreign_field_name,
                                    foreign_schema_name,
                                    foreign_entity_unique_field_names)
    end
  end

  return endpoints
end


local Endpoints = { handle_error = handle_error }


function Endpoints.new(schema, endpoints, prefix)
  return generate_endpoints(schema, endpoints, prefix)
end


return Endpoints
