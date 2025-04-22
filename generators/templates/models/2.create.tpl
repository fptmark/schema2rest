class ${Entity}Create(BaseModel):
    ${RegularFieldDefs}

    ${FieldValidators}
    class Config:
        orm_mode = True
        extra = Extra.ignore
