@property
def _id(self) -> Optional[str]:
    return self.id

@_id.setter
def _id(self, value: Optional[str]) -> None:
    self.id = value

async def save(self):
    # get the Elasticsearch client
    es = Database.get_es_client()
    if not es:
        raise RuntimeError("Elasticsearch client not initialized — did you forget to call Database.init()?")
 
    # save any autoupdate fields
    {{AutoUpdateLines}}

    # serialize & index
    body = self.model_dump(by_alias=True, exclude={"id"})
    resp = await es.index(
        index=self.__index__,
        id=self.id,
        document=body,
        refresh="wait_for",
    )
    self.id = resp["_id"]
    return self