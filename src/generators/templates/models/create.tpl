class {{Entity}}Create(BaseModel):
    id: str | None = Field(default=None)
    {{BaseFields}}

    model_config = ConfigDict(
        from_attributes=True,
        validate_by_name=True,
        use_enum_values=True
    )

