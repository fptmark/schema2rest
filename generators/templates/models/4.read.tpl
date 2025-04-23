class {Entity}Read(BaseModel):
    id: PydanticObjectId = Field(alias="_id")
    {BaseFields}
    {AutoFields}
            
    class Config:
        orm_mode = True
        allow_population_by_field_name = True
        json_encoders = {PydanticObjectId: str}
