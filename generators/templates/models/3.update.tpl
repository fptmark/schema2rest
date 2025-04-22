class ${Entity}Update(BaseModel):
${RegularFieldDefs}

    ${FieldValidators}
    class Config:
        orm_mode = True
        extra = Extra.ignore
