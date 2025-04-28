class {Entity}Update(BaseModel):
    {BaseFields}

    {Validators}
    
    model_config = {
        "from_attributes": True,
        "validate_by_name": True
    }
