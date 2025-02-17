import sys
import schemaConvert
import gen_db
import gen_main
import gen_models
import gen_routes
import update_indicies

if __name__ == "main":
    if len(sys.argv) < 2:
        print("Usage: python schema2rest.py <schema.yaml>")
        exit(-1)

    schema_file = sys.argv[1]
    path_root = ".'"

    # convert the schema to a format that can be used by the generators
    schema = schemaConvert.convert_schema(schema_file, path_root)
    if schema:

        #update the indexes
        update_indicies.run(schema)

        # generate the db.py file
        gen_db.generate_db(schema, path_root)

        # generate the main.py file
        gen_main.generate_main(schema, path_root)

        # generate the models
        gen_models.generate_models(schema, path_root)

        # generate the routes
        gen_routes.generate_routes(schema, path_root)