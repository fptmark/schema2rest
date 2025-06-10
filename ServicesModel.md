# Authentication Service Model Summary

## Summary of Auth Service-Related Files

| File                     | Role / Purpose                                                                                                                  | Code Generation Time          | Runtime                       |
|--------------------------|---------------------------------------------------------------------------------------------------------------------------------|------------------------------|-------------------------------|
| **`schema.yaml`**        | Declares usage of the auth service (`auth.cookies.redis`) on entities (User, Account). Drives codegen to generate service routes. | Yes — codegen reads this to generate service route scaffolding and validation contracts. | No — schema is metadata only. |
| **`base_model.py`**      | Defines Pydantic response models (`LoginResponse`, `LogoutResponse`, `RefreshResponse`) with metadata decorators for endpoints. | Yes — used by codegen to know the response models for auth endpoints.                  | Yes — response models used at runtime by FastAPI. |
| **`base_router.py`**     | Abstract base classes defining the authentication contract: methods like `login()`, `logout()`, `refresh()`, `authenticate()`. | Yes — codegen reads this to enforce contract conformance on concrete implementations.  | No — abstract, but foundational for runtime inheritance. |
| **`redis_provider.py`**  | Concrete implementation of cookie-based session storage using async Redis. Implements session create/get/delete/renew and auth logic. | No — handwritten concrete service implementation.                                      | Yes — core runtime service implementing auth.  |
| **`decorators.py`**      | Utility decorators (`@expose_endpoint`, `@expose_response`) used for attaching metadata to service methods and models.          | Yes — used during codegen and runtime for generating and exposing endpoints and responses. | Yes — used by FastAPI runtime for endpoint exposure metadata. |
| **`gen_service_routes.py`** | Service generator script that reads schema and base service files to generate concrete service route implementations.           | Yes — runs at code generation time to produce concrete route files.                    | No — generator script only. |

---

## Detailed Explanation

### 1. Code Generation Time

- **`schema.yaml`**  
  Declares the `auth.cookies.redis` service usage on entities. The code generator (`gen_service_routes.py`) uses this to find which entities require auth services and generate appropriate API route scaffolding.

- **`base_model.py`**  
  Provides Pydantic models for the service responses, annotated with decorators that the generator and FastAPI use to expose response schemas for `/login`, `/logout`, `/refresh`.

- **`base_router.py`**  
  Declares abstract base classes and method signatures for authentication workflows (login, logout, refresh, authenticate). This contract ensures the concrete implementations conform in method signatures and are correctly wired.

- **`decorators.py`**  
  Provides decorators that mark classes and methods with metadata for the generator and runtime to recognize which endpoints and response models to expose.

- **`gen_service_routes.py`**  
  The generator script itself runs only at code generation time, dynamically loading base classes, models, and concrete providers (like Redis auth), verifying contract conformance, and generating the final service route implementation files for each entity.

---

### 2. Runtime

- **`redis_provider.py`**  
  This file contains the **concrete implementation** of the authentication service backed by Redis. It manages session storage, cookie handling, session lifecycle, and authenticates requests by verifying sessions stored in Redis. This is the critical runtime component providing the actual auth logic.

- **`base_model.py`**  
  Pydantic models here are used at runtime by FastAPI for request validation and response serialization.

- **`decorators.py`**  
  Decorators also function at runtime to attach metadata used by FastAPI or other frameworks to expose endpoints and define API docs.

- **Generated service route files** (produced by `gen_service_routes.py` during codegen)  
  These files implement the API endpoints, wrapping calls to the Redis-backed auth provider. They are executed at runtime to handle auth HTTP requests.

---

## Workflow Summary

1. At **code generation time**, the generator (`gen_service_routes.py`) loads the schema, base models, abstract routers, and concrete service providers (like `redis_provider.py`), checks that concrete classes implement the abstract interface, and generates concrete route files that implement the API endpoints.

2. At **runtime**, the FastAPI app uses the generated route files, which invoke the concrete auth provider (`CookiesAuth` in `redis_provider.py`) to handle login/logout/refresh/authenticate flows via Redis session persistence. The Pydantic models from `base_model.py` define the request/response schemas.

---

## Next Steps

- Map how each method (login/logout/refresh/authenticate) is implemented and wired in detail.
- Show an example of generated service route code from your generator.
- Explain how the decorators tie into FastAPI or your framework.
- Assist in extending or modifying this auth service.

---

Does this cover what you need, or is there any part of this auth service model you'd like me to explore further or clarify?
