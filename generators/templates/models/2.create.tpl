class {Entity}Create(BaseModel):
    {BaseFields}

    {Validators}
    class Config:
        orm_mode = True
        extra = Extra.ignore
