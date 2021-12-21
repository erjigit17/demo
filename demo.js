 // fastify framework
 // orm sequelize
 
  
 // пример универсального эндпоинта 
  // GET /requests/custom
  // noinspection JSCheckFunctionSignatures
  fastify.route({
    method: 'GET',
    url: '/',
    preHandler: fastify.auth([fastify.verifyWebAuth]),
    schema: {
      tags: tag,
      summary: 'Получить список заявок',
      description:
        'Метод должен выдавать заявки в зависимости от query params. \n\n',
      produces: ['application/json'],
      security: bearer,
      headers: {$ref: 'defaultHeaders#'},
      querystring: {
        type: 'object',
        required: ['type'],
        additionalProperties: false,
        properties: {
          type: {
            type: 'string',
            description: 'Тип заявки',
            enum: ['CUSTOM', 'SEARCH_SERVICE', 'SEARCH_PRODUCT']
          },
          kind: {
            type: 'string',
            description: `Виды заявок:
            VACANT - все заявки со статусом CREATED,
            MY - все заявки созданные тем кто вызывает метод,
            ASSIGNEES - все заявки где числится как исполнитель,
            ALL - абсолютно все заявки, без исключения.`,
            enum: ['VACANT', 'MY', 'ASSIGNEES', 'ALL']
          }
        }
      },
      response: {
        200: {
          type: 'array', description: 'Массив заявок.', items: customRequestDetailDtoSchema // TODO сделать универсальную JSON схему
        },
        400: {$ref: 'badRequestError#'},
        404: {$ref: 'notFoundError#'},
        500: {$ref: 'internalServerError#'}
      }
    },
    handler: async (req, resp) => {
      const user = req.requestContext.get('user')
      const params = {}
      const sequelize = fastify.db.sequelize
      switch (req.query.kind) {
        case 'My':
          params.createdById = user._id
          break
        case 'VACANT':
          params[Op.or] = [ //WHERE ("Request"."status" = 0 OR ("Request"."status" = 10 AND jsonb_array_length("assignees") < "max_amount_of_proposals"));
            {status: requestStatus.CREATED},
            {
              [Op.and]: [
                {status: requestStatus.IN_PROCESS},
                sequelize.where(sequelize.fn('jsonb_array_length', sequelize.col('assignees')), {[Op.lt]: sequelize.col('max_amount_of_proposals')})
              ]
            }
          ]
          break
        case 'ASSIGNEES':
          params.assignees = {[Op.contains]: user._id}
          break
        case 'ALL':
      }

      const type = req.query.type.toLowerCase()
      const requests = await getRequests(params, type)
      if (!requests) throw fastify.httpErrors.notFound('Заявки не найдены')
      return resp.code(200).send(requests)
    }
  })
  
  // кусок плагина для запросов базу
  const dbHelpers = {
    getRequests: async (params, type) => {
    const requests = await Request.findAll({where: {...params}, include: [
        {model: RequestDetailsCustom, as: type, raw: true} // TODO менять модель в зависимости от типа заявки
      ]})
    if(requests.length === 0) return null
    return requests.map(request => {
      const result = JSON.parse(JSON.stringify(request))
      delete result[type]
      return {request: result, details: request[type]}
    })
   }
   
   
   //_______________________________________парсер, качает csv и записывает в postgres по расписанию________
   async function parseLast30DaysReport(url, clientId) {

  const json = await downloadAndConvert(url)
  const schema = reportLast30DaysSchema
  const arrOfObjs = convertKeysOfObjToCamelCase(deleteEmptyRows(json))
  //  удаление дней, которые есеть в базе
  const data = await deleteExistingDays(arrOfObjs, clientId)
  const params = {clientId: clientId}

  const parsedData = makeCorrectTypeOfValue(data, schema, params)

  await ReportSellerBoardLast30Days.bulkCreate(parsedData)
}

async function downloadAndConvert(url) {
  // скачивание
  needle.defaults({follow_max: 5}) // для решения проблем с переадресацией
  const data = await needle('get', url)
  // конвертирование csv в json
  const transform = csv()
  transform.end(data.raw)
  const json = []
  for await (const item of transform) json.push(item)

  return json
}

function convertKeysOfObjToCamelCase(json) {
  return json.map(obj => {
    // из ключей удаляем все символы кроме букв на латинице, стандарт камелкейс
    return Object.keys(obj).reduce((acc, key) => {
      let value = obj[key]
      return {...acc, [snakeToCamel(escape(key))]: value}
    }, {})

  })
}

function makeCorrectTypeOfValue(arrOfObjs, schema, params){
  return arrOfObjs.map(obj =>{
      // сборка правильного объекта с правильными типами
      const correctObject = Object.keys(schema).reduce((acc, key) => {
        const type =  schema[key].type
        const value = obj[key]
        return {...acc, [key]: (type === 'number') ? stringToNumber(value) : value || null}
      }, {})
    // перевод даты в стандарт удобный для Postgre
    if (correctObject.date) correctObject.date = transformDateFormat(correctObject.date)
    return {...correctObject, ...params}// добавление передппнных параметров
  })
}

function logger(arrOfObjs, schema, result) {
  const schemaKeys = Object.keys(schema)
  const objKeys = Object.keys(arrOfObjs[0])
  const newKeys = objKeys.filter(key => !schemaKeys.includes(key))

  return `Спарсено ${result.length} строк, столоб ${objKeys.length} из ${schemaKeys.length}, новых ${newKeys.length}`
}

function deleteEmptyRows(arrOfObjs) {
  // удаляем пустые строки
  return arrOfObjs.filter(obj => Object.values(obj).some(_ => _ !== ''))
}

function snakeToCamel(str){
  return     str.toLowerCase().replace(/([-_][a-z])/g, group =>
    group
      .toUpperCase()
      .replace('-', '')
      .replace('_', '')
  )
}

function escape(str){
  return str.replace(/["%,.]/g, ' ')
            .trim()
            .replace(/[^a-zA-ZА]/gi,'_')
            .replace(/__/g, '_')
            .toLowerCase()
}

function transformDateFormat(str) {
  const arr = str.split('.')
  const result = `${arr[2]}-${arr[1]}-${arr[0]}`
  return (arr.length === 3) ? result : str
}

function stringToNumber(str) {
  return (str !== '') ? parseInt(str) : null
}
   

