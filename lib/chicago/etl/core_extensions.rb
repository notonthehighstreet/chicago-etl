class Hash
  def put(key, value)
    store(key, value)
    self
  end

  def modify_existing(key)
    value = self[key]
    self[key] = yield value unless value.nil?
    self
  end
end
