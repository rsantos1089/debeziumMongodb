/**
 * A transforms incoming audit log event to BQ schema event
 * @param {string} inJson
 * @return {string} outJson
 */
function transformAuditLogEvent(inJson) {
    var original = JSON.parse(inJson)

    var now = new Date();

    var transformed = original;

    transformed.id_pais = transformed.id_pais.toString()
    transformed.id_empresa = transformed.id_empresa.toString()
    transformed.id_sistema = transformed.id_sistema.toString()
    transformed.fecha_carga = now.toISOString().substring(0, 10) + " " + now.toISOString().substring(11, 19)
	transformed.partition_date = now.toISOString().substring(0, 10)
    transformed.payload = JSON.stringify(transformed.payload)

    return JSON.stringify(transformed);
}

// var input = {
//     id_pais: "93",
//     id_empresa: "10",
//     id_sistema: "10",
//     track_id: "684bbfbe-c2c3-459e-82e7-984d79586637",
//     accion: "CREAR",
//     source: "LEGO_RIESGO",
//     fecha_publicacion: "2021-07-06 22:45:26",
//     fecha_creacion: "2021-07-06 22:45:26",
//     fecha_ultima_actualizacion: "2021-07-06 22:45:26",
//     payload: {
//         riesgo: {
//             field_1: "value_1",
//             field_2: "value_2"
//         }
//     }
// }
//
// console.log(transformAuditLogEvent(JSON.stringify(input)));