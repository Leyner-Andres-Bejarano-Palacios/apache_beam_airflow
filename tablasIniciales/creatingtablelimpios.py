from google.cloud import bigquery
from google.cloud.exceptions import NotFound

client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to determine existence.
table_id_dimPensionados = "afiliados-pensionados-prote.Datamart.dimPensionados"
table_id_dimEstado = "afiliados-pensionados-prote.Datamart.dimEstado"
table_id_dimTipoSolicitud = "afiliados-pensionados-prote.Datamart.dimTipoSolicitud"
table_id_dimBeneficiarios = "afiliados-pensionados-prote.Datamart.dimBeneficiarios"
table_id_dimInfPersonas = "afiliados-pensionados-prote.Datamart.dimInfPersonas"

table_id_dimEstadoBono = "afiliados-pensionados-prote.Datamart.dimEstadoBono"
table_id_factSolicitudes = "afiliados-pensionados-prote.Datamart.factSolicitudes"
table_id_dimEntidadEmisorBono = "afiliados-pensionados-prote.Datamart.dimEntidadEmisorBono"
table_id_factCalculosActuariales = "afiliados-pensionados-prote.Datamart.factCalculosActuariales"
table_id_factBonos = "afiliados-pensionados-prote.Datamart.factBonos"

table_id_dimEstadoCupon = "afiliados-pensionados-prote.Datamart.dimEstadoCupon"
table_id_dimCuponBono = "afiliados-pensionados-prote.Datamart.dimCuponesBono"
table_id_dimTiempo = "afiliados-pensionados-prote.Datamart.dimTiempo"
table_id_factPagos = "afiliados-pensionados-prote.Datamart.factPagos"
table_id_factDefiniciones = "afiliados-pensionados-prote.Datamart.factDefiniciones"

table_id_factAhorroPensional = "afiliados-pensionados-prote.Datamart.factAhorroPensional"
table_id_dimTipoPension = "afiliados-pensionados-prote.Datamart.dimTipoPension"
table_id_dimConceptoPago = "afiliados-pensionados-prote.Datamart.dimConceptoPago"
table_id_dimEstadoPago = "afiliados-pensionados-prote.Datamart.dimEstadoPago"

#Creation table dimPensionados or validation already exist
try:
    client.get_table(table_id_dimPensionados)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimPensionados))

except NotFound:
    print("Table {} is not found.".format(table_id_dimPensionados))

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimPensionados = "afiliados-pensionados-prote.Datamart.dimPensionados"

    schemacleanpen = [
        bigquery.SchemaField("FechaDelDato", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("PensionadosID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ModalidadPension", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("IndicadorPensionGPM", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("NumeroSolicitudPension", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tipoDePension", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EstadoPension", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ModalidadInicial", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("DocumentoDeLaPersona", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoDocumentoDeLaPersona", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoDePersona", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechaPension", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechaSiniestro", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechaNacimiento", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Sexo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Nombre", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Mesadas", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_dimPensionados, schema=schemacleanpen)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

#Creation table table_id_dimEstado or validation already exist

try:
    client.get_table(table_id_dimEstado)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimEstado))
except NotFound:
    print("Table {} is not found.".format(table_id_dimEstado))

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimEstado = "afiliados-pensionados-prote.Datamart.dimEstado"

    schemacleanestado = [
        bigquery.SchemaField("EstadoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Estado", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Atributo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Descripcion", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_dimEstado, schema=schemacleanestado)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

#Creation table table_id_dimTipoSolicitud or validation already exist

try:
    client.get_table(table_id_dimTipoSolicitud)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimTipoSolicitud))
except NotFound:
    print("Table {} is not found.".format(table_id_dimTipoSolicitud))

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimTipoSolicitud = "afiliados-pensionados-prote.Datamart.dimTipoSolicitud"

    schemacleantipoSolicitud = [
        bigquery.SchemaField("TipoSolicitudID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoSolicitud", "STRING", mode="NULLABLE"),

    ]

    table = bigquery.Table(table_id_dimTipoSolicitud, schema=schemacleantipoSolicitud)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


#Creation table table_id_dimBeneficiarios or validation already exist


try:
    client.get_table(table_id_dimBeneficiarios)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimBeneficiarios))
except NotFound:
    print("Table {} is not found.".format(table_id_dimBeneficiarios))

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimBeneficiarios = "afiliados-pensionados-prote.Datamart.dimBeneficiarios"


    schemacleanben = [
        bigquery.SchemaField("BeneficiariosId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoIdentificacionBeneficiario", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("IdentificacionBeneficiario", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechaDato", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("IdentificacionAfiliado", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("fechaFinTemporalidad", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ConsecutivoPension", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SecuenciaBeneficiario", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CalidadBeneficiario", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Sexo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechasNacimiento", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoBeneficiario", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SubtipoBeneficiario", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Parentesco", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Nombre", "STRING", mode="NULLABLE"),

    ]

    table = bigquery.Table(table_id_dimBeneficiarios, schema=schemacleanben)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


#Creation table table_id_dimInfPersonas or validation already exist


try:
    client.get_table(table_id_dimInfPersonas)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimInfPersonas))
except NotFound:
    print("Table {} is not found.".format(table_id_dimInfPersonas))

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimInfPersonas = "afiliados-pensionados-prote.Datamart.dimInfPersonas"

    schemacleanInfPersona = [
        bigquery.SchemaField("InfPersonasID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("PensionadosId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("BeneficiariosId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoPersona", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SolicitudesId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CalculosActuarialesId", "STRING", mode="NULLABLE"),

    ]

    table = bigquery.Table(table_id_dimInfPersonas, schema=schemacleanInfPersona)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


#Creation table table_id_dimEstadoBono or validation already exist

try:
    client.get_table(table_id_dimEstadoBono)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimEstadoBono))
except NotFound:
    print("Table {} is not found.".format(table_id_dimEstadoBono))

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimEstadoBono = "afiliados-pensionados-prote.Datamart.dimEstadoBono"

    schemacleanEstadoBono = [
        bigquery.SchemaField("EstadoBonoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EstadoBono", "STRING", mode="NULLABLE")
    ]

    table = bigquery.Table(table_id_dimEstadoBono, schema=schemacleanEstadoBono)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


#Creation table table_id_dimEstadoBono or validation already exist

try:
    client.get_table(table_id_factSolicitudes)  # Make an API request.
    print("Table {} already exists.".format(table_id_factSolicitudes))
except NotFound:
    print("Table {} is not found.".format(table_id_factSolicitudes))

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_factSolicitudes = "afiliados-pensionados-prote.Datamart.factSolicitudes"

    schemacleansolicitudes = [
        bigquery.SchemaField("TiempoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EstadoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Numero_Solicitud", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoSolicitudID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("InfPersonasID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("validacionDetected", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_factSolicitudes, schema=schemacleansolicitudes)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


#Creation table table_id_dimEntidadEmisorBono or validation already exist

try:
    client.get_table(table_id_dimEntidadEmisorBono)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimEntidadEmisorBono))
except NotFound:
    print("Table {} is not found.".format(table_id_dimEntidadEmisorBono))

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimEntidadEmisorBono = "afiliados-pensionados-prote.Datamart.dimEntidadEmisorBono"

    schemacleanentidademisorbono = [
        bigquery.SchemaField("TipoIdEntidadEmisora", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EntidadEmisoraBonoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("IdEntidadEmisora", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_dimEntidadEmisorBono, schema=schemacleanentidademisorbono)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

#Creation table table_id_factCalculosActuariales or validation already exist

try:
    client.get_table(table_id_factCalculosActuariales)  # Make an API request.
    print("Table {} already exists.".format(table_id_factCalculosActuariales))
except NotFound:
    print("Table {} is not found.".format(table_id_factCalculosActuariales))

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_factCalculosActuariales = "afiliados-pensionados-prote.Datamart.factCalculosActuariales"

    schemacleancalculosactuariales = [
        bigquery.SchemaField("CalculosActuarialesID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TiempoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("InfPersonasID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("fechaDato", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("InteresTecnico", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("InflacionLargoPlazo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FactorDeslizamiento", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("AjusteBeneficiarios", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Comision", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SupuestoDeBeneficiarios", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_factCalculosActuariales, schema=schemacleancalculosactuariales)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


#Creation table table_id_factBonos or validation already exist

try:
    client.get_table(table_id_factBonos)  # Make an API request.
    print("Table {} already exists.".format(table_id_factBonos))
except NotFound:
    print("Table {} is not found.".format(table_id_factBonos))
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_factBonos = "afiliados-pensionados-prote.Datamart.factBonos"

    schemacleanbonos = [
        bigquery.SchemaField("Inf_PersonasId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechaDato", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Bono", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Tasa", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechaRedencionBono", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FaltanteBonoPensional", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EstadoBonoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EmisorBonoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("VersionBono", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CalculosActuarialesID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CuponesBonoID", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_factBonos, schema=schemacleanbonos)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


#Creation table table_id_dimEstadoCupon or validation already exist


try:
    client.get_table(table_id_dimEstadoCupon)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimEstadoCupon))
except NotFound:
    print("Table {} is not found.".format(table_id_dimEstadoCupon))
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimEstadoCupon = "afiliados-pensionados-prote.Datamart.dimEstadoCupon"

    schemacleanestadocupon = [
        bigquery.SchemaField("EstadoCuponID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EstadoCupon", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_dimEstadoCupon, schema=schemacleanestadocupon)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

#Creation table table_id_dimCuponesBono or validation already exist


try:
    client.get_table(table_id_dimCuponBono)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimCuponBono))
except NotFound:
    print("Table {} is not found.".format(table_id_dimCuponBono))
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimCuponBono = "afiliados-pensionados-prote.Datamart.dimCuponBono"

    schemacleancuponbono = [
        bigquery.SchemaField("CuponesBonoId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ConsecutivoDeCupon", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EstadoCuponID", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_dimCuponBono, schema=schemacleancuponbono)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

#Creation table table_id_dimTiempo or validation already exist


try:
    client.get_table(table_id_dimTiempo)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimTiempo))
except NotFound:
    print("Table {} is not found.".format(table_id_dimTiempo))
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimTiempo = "afiliados-pensionados-prote.Datamart.dimTiempo"

    schemacleantiempo = [
        bigquery.SchemaField("date", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Anno", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Semana", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Trimestre", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_dimTiempo, schema=schemacleantiempo)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


#Creation table table_id_factPagos or validation already exist

try:
    client.get_table(table_id_factPagos)  # Make an API request.
    print("Table {} already exists.".format(table_id_factPagos))
except NotFound:
    print("Table {} is not found.".format(table_id_factPagos))

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_factPagos = "afiliados-pensionados-prote.Datamart.factPagos"

    schemacleanpagos = [
        bigquery.SchemaField("TiempoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("InfPersonasID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoPensionID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ConceptoPagoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EstadoPago", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechaDato", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ValorPagoAnterior", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ValorPago", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechaGeneracionPago", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechaPago", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("anno", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mes", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("NombreDestinatarioPago", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoIdentificacionDestinatarioPago", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("IdentificacioDestinatarioPago", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_factPagos, schema=schemacleanpagos)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

#Creation table table_id_factDefiniciones or validation already exist

try:
    client.get_table(table_id_factDefiniciones)  # Make an API request.
    print("Table {} already exists.".format(table_id_factDefiniciones))
except NotFound:
    print("Table {} is not found.".format(table_id_factDefiniciones))
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_factDefiniciones = "afiliados-pensionados-prote.Datamart.factDefiniciones"

    schemacleandefiniciones = [
        bigquery.SchemaField("InfPersonasID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoPensionID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoPension", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechaModificacion", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SemanasMomentoDefinicion", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SemanasProteccion", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SemanasAFP", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SemanasBono", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("modalidadPension", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_factDefiniciones, schema=schemacleandefiniciones)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

#Creation table table_id_factAhorroPensional or validation already exist

try:
    client.get_table(table_id_factAhorroPensional)  # Make an API request.
    print("Table {} already exists.".format(table_id_factAhorroPensional))
except NotFound:
    print("Table {} is not found.".format(table_id_factAhorroPensional))
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_factAhorroPensional = "afiliados-pensionados-prote.Datamart.factAhorroPensional"

    schemacleanahorropensional = [
        bigquery.SchemaField("AhorroPensionalID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TiempoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("InfPersonasID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FechaDato", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("saldoPension", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("SumaAdicional", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CapitalNecesario", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("CapitalNecesarioSMLV", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FaltanteCapital", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("FaltanteSumaAdicional", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("anno", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("mes", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_factAhorroPensional, schema=schemacleanahorropensional)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

#Creation table table_id_dimTipoPension or validation already exist



try:
    client.get_table(table_id_dimTipoPension)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimTipoPension))
except NotFound:
    print("Table {} is not found.".format(table_id_dimTipoPension))
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimTipoPension = "afiliados-pensionados-prote.Datamart.dimTipoPension"

    schemacleantipopension = [
        bigquery.SchemaField("TipoPensionID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("TipoPension", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_dimTipoPension, schema=schemacleantipopension)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

#Creation table table_id_dimConceptoPago or validation already exist

try:
    client.get_table(table_id_dimConceptoPago)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimConceptoPago))
except NotFound:
    print("Table {} is not found.".format(table_id_dimConceptoPago))
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimConceptoPago = "afiliados-pensionados-prote.Datamart.dimConceptoPago"

    schemacleanconceptopago = [
        bigquery.SchemaField("ConceptoPagoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ConceptoPago", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_dimConceptoPago, schema=schemacleanconceptopago)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

#Creation table table_id_dimEstadoPago or validation already exist

try:
    client.get_table(table_id_dimEstadoPago)  # Make an API request.
    print("Table {} already exists.".format(table_id_dimEstadoPago))
except NotFound:
    print("Table {} is not found.".format(table_id_dimEstadoPago))

    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id_dimEstadoPago = "afiliados-pensionados-prote.Datamart.dimEstadoPago"

    schemacleanestadopago = [
        bigquery.SchemaField("EstadoPagoID", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EstadoPagoDescripcion", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("EstadoPago", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id_dimEstadoPago, schema=schemacleanestadopago)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )








