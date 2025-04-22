class ${Entity}Read(BaseModel):
    id: PydanticObjectId = Field(alias="_id")
    ${RegularFieldDefs}
    ${AutoFieldDefs}

    class Config:
        orm_mode = True
        allow_population_by_field_name = True
        json_encoders = {PydanticObjectId: str}
