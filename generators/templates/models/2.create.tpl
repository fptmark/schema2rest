class {Entity}Create(BaseModel):
    {BaseFields}

    {Validators}
    
    model_config = {
        "from_attributes": True,
        "validate_by_name": True
    }
