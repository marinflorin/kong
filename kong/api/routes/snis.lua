local function not_found(self, db, helpers)
  return helpers.responses.send_HTTP_NOT_FOUND()
end

return {
  -- GET / PATCH / DELETE /snis/sni are the only methods allowed

  ["/snis"] = {
    before = not_found,
  },

  ["/snis/:snis/certificate"] = {
    before = not_found,
  },

}
