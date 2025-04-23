class {Entity}Update(BaseModel):
    {BaseFields}

    {Validators}
    class Config:
        orm_mode = True
        extra = Extra.ignore
