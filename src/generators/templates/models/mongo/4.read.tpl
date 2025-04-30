class {{Entity}}Read(BaseModel):
    id: PydanticObjectId = Field(alias="_id")
    {{BaseFields}}
    {{AutoFields}}
            
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        json_encoders={PydanticObjectId: str},
    )
