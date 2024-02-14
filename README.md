# Рефакторим обработку потоков данных. ClickHouse+.NET channels

Помните, что все сказанное ниже решало конкретную ситуацию.\
Бездумная копипаста приведенных советов может и принесет вам много боли.

> ! Важно\
> Данные. Данные обладают некоторой комбинаторностью. Можно по разному их группировать и менять структуру, пока это не нарушает логику. За счет этого достиглась хорошая экономия.\
> Код. [с гордостью в голосе] Это .Net Framework 4.8 с устаревшими либами! Если вы пришли сюда за span'ами и stream api, то можете закрывать статью. Работаем с тем, что есть и некоторые места будут выглядеть уродливо.

<details>
    <summary>Статья за 10 секунд</summary>

Перешли с mysql на clickhouse. Написали нормальный код.\
Ускорили время работы с 60 до 2 минут, избавились от аллокаций со 100 до 2 ГБ.\
Уменьшили потребление памяти для ин-мемори кэшей в 10 раз.

Как этого добиться:

1. минимально подходящие типы и not null.
2. аггрегация на стороне бд. хотя бы частично*
3. фильтрация ПЕРЕД записью в бд. по возможности*
4. параллельная обработка. паттерн producer-consumer
5. уменьшение аллокаций и попадания в LOH:
    * пулы и буфферы. ArrayPool, RecyclableMemoryStream.
    * структуры вместо классов.
    * обработка маленьких блоков.
    * отказ от промежуточных DTO или автомаппера*
    * FrugalObject*
6. библиотеки под задачу. zstd, zeroFormatter\msgpack

Все пункты выше - здравый смысл и советы умных книжек, докладов с Dot.Next и статей с Хабра\Medium. А все ниже демострация того, как их использовать на чем-то интереснее консольного приложения.

Советы и замечания по коду или статье прошу писать в issue на гите.
Если будут серьезные замечаения, планирую апдейтить данную статью в течении недели.

</details>

## Какую задачу решал код?

<p style="text-align: center;">
    <img src="graph/main.png" alt="common pipeline view"/>
    <figcaption style="font-size: smaller; color: gray; text-align: center;">Общая схема этапов</figcaption>
</p>

* ``A, save stat`` читает данные с некоторой ``queue`` и пишет в БД в один поток.\
* ``B, make model`` вычитывает последние 200 000 000 строк, фильтрует, группирует, делает расчет и отправляет результат в БД.

> Назовем это модель. Часть считается на C#, более сложные на Python.

* ``C, pack`` делает из модели справочник. Далее сериализация-стажитие-отправка бинарника в Redis.\
* ``C, unack`` получение бинари из Rdis-десериализация-растажитие. Далее обновление ин-мемори справочник сервиcа ``E, calc``.\
Этот шаг помогает забирать гигабайтные справочники очень быстро.

> Справочник - key-value структура ``Dictionary<TKey, TValue>``.\
> Бинарник - сериализованный и сжатый справочник.

* ``E, calc`` использует полученные справочники для своей работы.

> Распределенные кеши aka Redis не подошли из-за задержек на де\сереализацию и сеть.

## Почему понадобился рефакторинг?

Ниже график для кандидата на сегодня. А на подходе был еще один, в 5 раз больше!\
Докупить террабайт оперативки не получилось, пришлось рефакторить.

<p align="center">
    <img src="img/clinet-server-old-new-2.png" alt="real app"/>
    <figcaption style="font-size: smaller; color: gray; text-align: center;"> Потребление RAM и время работы. Слева client С, unpack. Справа server B-C. </figcaption>
</p>

Слева трудяга-сервер.

* В 14:30 начал грузить данные и строить модель.
* В 15:20 начал паковать бинарник и закончил в 15:30.

> Цифры немного врут. Реально этот монстр жрет в private set все 256 GB RAM и тянет свои лапки в файл подкачки (если неделю его не трогать, то падает с out of memory).

* В 16:05 после рестарта запуск версии Bv2 + Cv3

Справа 20+ аппов и их потребление RAM при подгруженных кэшах.

* В 15:30 обновление справочника с Redis.
* В 16:30 после рестарта запуск версии Bv2 + Cv3

<p align="center">
    <img src="img/ref3.png" alt="real app"/>
    <figcaption style="font-size: smaller; color: gray; text-align: center;"> Время работы и потребление RAM\трафика. Для разных верчсий </figcaption>
</p>

## Правим схемы, перезжаем в кликхаус

<details>
    <summary>замечание о строке Tags</summary>

* pattern: ([A-Z0-9]+_?)+, example 14_974_53A_386, в 99% случаев массив uint16
* еncode: utf8 (mysql, clickhouse), utf16 (C#)
* length: 24
* sub count: 4
* approximate size: 20 B (mysql, clickhouse), 40 B (C#)

```log
str_len   = [0.1, 0.25, 0.5, 0.75, 0.9]
quantile  = [  9,   14,  19,   21,  27]
sub count = [  1,    3,   3,    4,   5]
```

Допущение, строки будем считать в кодировке utf8 и для БД и для C#.

</details>

### A, save stat

> У сервиса A, save stat потребляемая схема не менялась.

Сюда переехали фильтры из сервиса B, make model. ``[3] - фильтруй на стороне БД``.\
Возможно вам этот вариант вам не подойдет, т.к. вы хотите иметь все исторические данные или не устроит задержка в реакции на фильтр. Если добавлялся новый фильтр, то данные продолжали тянуться, просто на расчетах получали для них 0. Если фильтр удалялся, вполне устраивало, что новые данные появяться только в следущий час.

### B, make model

| column name       | v1          | v2          | v3         |
|-------------------|------------:|------------:|-----------:|
| AppId             | 64+8        | 16          | 16         |
| BidId             | 64+8        | 16          | 16         |
| Tags              | 24\*8       | 24\*8       | 24\*8      |
| Type              | 64+8        | 8           | 8*40       |
| Unit              | 64+8        | 8           | 8*40       |
| Rate              | 128+8       | 32          | 32\*40     |
| Shift             | 128+8       | 16          | 16\*40     |
| Timestamp         | 64          | 64          | 0          |
| Area              | 64          | 0           | 0          |
| Id                | 64          | 0           | 0          |
| UselessSet        | 448         | 0           | 0          |
| row count         | 200 000 000 | 200 000 000 | 1 250 000  |
| aprox. row, bytes | 1328        | 352         | 2784       |
| aprox. size, GB   | 30,92       | 8,20        | 0,41       |

> 200M / 4 [filter by timestamp] / 40 [group by Tags, AppId, BidId] = 1.25M

* Приведем типы колонок в порядок
* Поставим not null. Всегда есть значение или default = min\0\max.
* UselessSet - не нужны для работы.
* Timestamp - нужен для фильтрации в группе, в v3 агрегируем на стороне КХ.
* Area - поле с низкой селективностью, всего 3 значения и занимает int64. \
Избавимся от него созданием трех таблиц с суффиксом-значением raw_Area.\
Можно было разбить на партиции или шарды (если есть).
* Type, Unit - были частью ключа в v2, в v3 идут с Rate, Shift в массиве (40 медианная длинна) группировкой в КХ. Далее по коду будет понятно, что за группировка

> ch появлися позднее, поэтому есть B-v2-mysql, но только C-v3-ch.

### С, pack

| column name       | v1         | v3         |
|-------------------|------------|------------|
| AppId             | 64         | 16         |
| BidId             | 64         | 16         |
| Tags              | 24\*8      | 24\*8      |
| Type              | 64         | 8\*40      |
| Unit              | 64         | 8\*40      |
| Shift             | 128        | 16\*40     |
| Area              | 64         | 0          |
| Id                | 64         | 0          |
| row count         | 50 000 000 | 1 250 000  |
| aprox. row, bytes | 704        | 1504       |
| aprox. size, GB   | 3,28       | 0,22       |

Сервис C, читает данные полученные от сервиса B, make model.
По аналогии правим типы колонок, выкидываем Area, используем массивы.

> B-v2-mysql + C-v3-ch. Вместо плоской схемы в mysql, сделали на массивах в ch.\
> Не использовали mysql blob, терялось удобстсво контроля за значениями модели.

### С, unpack & E, calc

| column name       | v1         | v3         |
|-------------------|------------|------------|
| -- dictionary --- | ---------- | ---------- |
| AppId             | 64         | 16         |
| BidId             | 64         | 16         |
| Tags              | 24\*16     | 24\*8      |
| Type              | 64         | 8\*40      |
| Unit              | 64\*8      | 8\*40      |
| Shift             | 128\*8     | 16\*40     |
| Area              | 64\*8      | 0          |
| Id                | 64         | 0          |
| row count         | 6 250 000  | 1 250 000  |
| aprox. row, bytes | 2688       | 1504       |
| aprox. size, GB   | 1,96       | 0,22       |
| -- binary ------- | ---------- | ---------- |
| aprox. size, GB   | 1,1        | 0,08       |

## B, make model. Code

Напоминаю тут .net framework 4.8.\
Для уменьшение кол-ва строк, код:\
Не ловит исключения.\ Но любое исключение отменяет другие задачи (если есть)
Не использует DI.\
Не использует конфиги.

Сейчас будет версия v1. Уберите детей от экранов.\
Классы, блокирующее чтения, linq где не надо, кортежи\строки как ключ и куча аллокаций.

### Bv1

* main

```C#
public override async Task Run()
{
  var modelCgf = _builder.GetModelConfigOld();
  var aggData = GetAggregated();                           // 30-50 min
  var modelData = _builder.CalcModelV1(aggData, modelCgf); // 10-20 min
  await _rep.InsertV1(modelData);                          // 10-15 min
}
```

<details>
<summary>select and aggregate</summary>

```C#
private IEnumerable<RawV1> GetAggregated()
{
  // лимит в 2 ГБ не даст положить в один лист 200+ млн
  var preAggregatedData = new List<IEnumerable<RawV1>>();
  var lastId = 0L;

  while (true)
  {
    var rawData = _rep.SelectV1(lastId, batch: 100_000);

    if (!rawData.Any()) break;

    var rawDt = rawData.Last();

    lastId = rawDt.Id;

    preAggregatedData.Add(_builder.GroupFilterV1(rawData));

    if (DateTime.UtcNow - rawDt.Ts <= TimeSpan.FromMinutes(10)) break;
  }

  return _builder.GroupFilterV1(preAggregatedData.SelectMany(x => x));
}
```

Чтобы вытянуть данные используем стразу два поля-хелпера Id и Timestamp.\
Чтение напрягает GC, блочит тред (да да, читаем синхронно) и занимает много времени.

</details>

<details>
<summary>select</summary>

```C#
public List<RawV1> SelectV1(long lastId, int batch)
{
  var sql = $@"SELECT
AppId, 
BidId, 
Tags, 
Type, 
Unit, 
Shift, 
Rate, 
Ts,
Id, 
Area 
FROM Raw 
WHERE Id > {lastId} 
ORDER BY Id 
LIMIT {batch}";

  using (IDbConnection db = new SqlConnection(_sqlStr))
  {
    return db.Query<RawV1>(sql).ToList();
  }
}
```

Ничего не мешало добавить async\await, но все же его тут нет.

</details>

<details>
<summary>aggregate</summary>

```C#
public IEnumerable<RawV1> GroupFilterV1(IEnumerable<RawV1> rawData)
{
  // group
  return rawData
  .GroupBy(rawDt => (
    rawDt.Area,
    rawDt.Unit,
    rawDt.BidId,
    rawDt.AppId,
    rawDt.Tags,
    rawDt.Type))
  .Select(group => {
    // filter
    var maxTs = group.Max(x => x.Ts);
    return group.Where(x => x.Ts == maxTs)
      .OrderBy(x => x.Shift).First();
  });
}
```

<p align="center">
    <img src="meme/cat.jpg" alt="real app"/>
    <figcaption style="font-size: smaller; color: gray; text-align: center;"> Нет слов, одни эмоции </figcaption>
</p>

По одному raw пытались строить несколько моделей, что само по себе ошибка.\
Используйте IEqualityComparer\<T\> под каждый случай, вместо копирования в кортежи.\
Но лучше своя модель + IEquatable\<T\> + override GetHashCode\\Equals.

Из-за отложенного выполнения быстрее возвращаемся к Select. Но храним все 200М в памяти, хотя группировка с фильтром оставят 50М. И все это уходит в LOH.

</details>

<details>
<summary>calc model</summary>

```c#
public List<ModelV1> CalcModelV1(IEnumerable<RawV1> aggData, ModelConfigOld modelCfg)
{
  return aggData
  .Join(modelCfg.Specific,
    key => $"{key.Unit}_{key.Area}",
    key => $"{key.Unit}_{key.Area}",
    (raw, specific) => new { raw, specific })
  .Where(s => s.specific.Enabled)
  .Join(modelCfg.Common,
    key => key.raw.AppId,
    key => key.OfTag,
    (a, common) => new { common, a.specific, a.raw })
  .Select(a => new ModelV1() {
      Area = a.raw.Area,
      Unit = a.raw.Unit,
      BidId = a.raw.BidId,
      AppId = a.raw.AppId,
      Tags = a.raw.Tags,
      Type = a.raw.Type,
      Shift = CalcValueOrZero(a.common, a.specific, a.raw)})
  .ToList();
}
```

Join можно переделать на словарь с нормальными ключами (без кортежей\строк).\
Огромная аллокация. 50М классов, куда просто скопировали все значения, кроме одного.

</details>

<details>
<summary>insert</summary>

```c#
public async Task InsertV1(List<ModelV1> modelData)
{
  using (var conn = new MySqlConnection(_sqlStr))
  {
    conn.Open();

    var bulk = new MySqlBulkCopy(conn) { DestinationTableName = "model" };

    var table = new DataTable();
    table.Columns.AddRange(new[]
    {
      new DataColumn("Id",    typeof(long)) { AllowDBNull = false },
      new DataColumn("Area",  typeof(long)) { AllowDBNull = false },
      new DataColumn("AppId", typeof(long)) { AllowDBNull = false },
      new DataColumn("BidId", typeof(long)) { AllowDBNull = false },
      new DataColumn("Tags",  typeof(long)) { AllowDBNull = false },
      new DataColumn("Type",  typeof(long)) { AllowDBNull = true },
      new DataColumn("Unit",  typeof(long)) { AllowDBNull = true },
      new DataColumn("Shift", typeof(decimal)) { AllowDBNull = false }
    });

    foreach (var modelDataBatch in modelData.Batch(size: 100_000))
    {
      foreach (var modelDt in modelDataBatch)
      {
        table.Rows.Add(
          0,
          modelDt.Area,
          modelDt.AppId,
          modelDt.BidId,
          modelDt.Tags,
          modelDt.Type,
          modelDt.Unit,
          modelDt.Shift
        );
      }

      await bulk.WriteToServerAsync(table);

      table.Clear();
    }
  }
}
```

DataTable можно переиспользовать, а не создавать на каждый batch.

</details>

### Bv2

```c#
public override async Task Run()
{
  var modelCfg = _builder.GetModelConfig();
  var areas = new[] { Area.ZoneA, Area.Konoha, Area.ZoneC };
  var aggData = new Dictionary<ModelKey, DynArr<ModelValV2>>(700_000_000);

  foreach (var area in areas)
  {
    await FillAggregated(aggData, area);                               // 10 min sum
    var modelDataLazy = _builder.CalcModelV2(aggData, modelCfg, area); // -- min sum
    await _rep.InsertV3(modelDataLazy, area);                          //  4 min sum
    aggData.Clear();
  }
}
```

CalModelV2 в lazy формате, тратит время во время вставки.

<details>
<summary>select and aggregate</summary>

```c#
private async Task FillAggregated(Dictionary<ModelKey, DynArr<ModelValV2>> aggData, Area area)
{
  BoundedChannelOptions opt = new BoundedChannelOptions(10_000)
  {
    SingleReader = true,
    SingleWriter = true,
    FullMode = BoundedChannelFullMode.Wait
  };

  var channel = Channel.CreateBounded<WrapV2>(opt);

  var readTask = Task.Run(async () =>
  {
    try
    {
      foreach (var rawDt in _rep.SelectV2(area, _cts.Token))
      {
        await channel.Writer.WriteAsync(rawDt, _cts.Token);
      }
    }
    finally
    {
      channel.Writer.TryComplete();
    }
  });

  var aggTask = Task.Run(async () =>
  {
    while (await channel.Reader.WaitToReadAsync(_cts.Token))
    {
      var rawDt = await channel.Reader.ReadAsync(_cts.Token);

      _builder.GroupFilterV2(aggData, ref rawDt);
    }
  });

  await Task.WhenAll(readTask, aggTask);
}
```

Используем каналы, чтобы не останаливать чтение, оно бутылочное горылшко.

</details>

<details>
<summary>select</summary>

```c#
public IEnumerable<WrapV2> SelectV2(Area area, CancellationToken tkn = default)
{
  var sql = $@"SELECT
AppId, 
BidId, 
Tags, 
Type, 
Unit, 
Shift, 
Rate, 
Ts,
FROM Raw{area} 
ORDER BY Id DESC";

  using (var con = new MySqlConnection(_chStr))
  using (var cmd = con.CreateCommand())
  {
    con.Open();

    cmd.CommandText = sql;
    cmd.Prepare();

    using (var rdr = cmd.ExecuteReader())
    {
      while (rdr.Read())
      {
        if (tkn.IsCancellationRequested)
          throw new OperationCanceledException();

        var i = -1;
        yield return new WrapV2 (
          key: new ModelKey (
            appId: rdr.GetFieldValue<ushort>(++i),
            bidId: rdr.GetFieldValue<ushort>(++i),
            tags: rdr.GetFieldValue<string>(++i)),
          val: new ModelValV2 (
            type: rdr.GetFieldValue<byte>(++i),
            unit: rdr.GetFieldValue<byte>(++i),
            shift: rdr.GetFieldValue<ushort>(++i),
            rate: rdr.GetFieldValue<int>(++i),
            ts: rdr.GetFieldValue<DateTime>(++i)));
      }
    }
  }
}
```

О важности размерности типов и ``Prepare`` для mysql.\
Движок отдает данные как TextRow или BinaryRow.

Первая форма **TextRow**, это Variable Length Encoding c сложным парсингом.\
Для колонки long, в котором byte получим 1 байт длинна + 1 байт данные.\
Нужно парсить через  медленный``System.Buffers.Text.Utf8Parser.TryParse``.

Вторая форма **BinaryRow**, отдает запись как есть.\
Для колонки long, в котором byte получим 4 байта данных.\
Парсим ``MemoryMarshal.Read<long>`` и маппим ``(byte)reader.GetFieldValue<long>(++i)``.

При ``nullable`` еще будем бегать в битовоую маску и проверять флаг наличия данных.

С версии 2.3.0-beta.3 структура чуть поменялась. Вместо двух классов отдельные ридеры:

[BinarySignedInt64ColumnReader](https://github.com/mysql-net/MySqlConnector/blob/master/src/MySqlConnector/ColumnReaders/BinarySignedInt64ColumnReader.cs)\
[TextSignedInt64ColumnReader](https://github.com/mysql-net/MySqlConnector/blob/master/src/MySqlConnector/ColumnReaders/TextSignedInt64ColumnReader.cs)

В моем случае, после вызова ``Prapare`` время чтения уменьшалось на 2-3 минуты.

Еще можно включить сжатие zstd. Снизили бы время чтения еще на 20-50%.
Не было поддержки на тот момент.

</details>

<details>
<summary>aggreagte</summary>

```c#
public void GroupFilterV2(Dictionary<ModelKey, DynArr<ModelValV2>> aggData, ref WrapV2 rawDt)
{
  // group
  if (aggData.TryGetValue(rawDt.Key, out var values))
  {
    for (var i = 0; i < values.Count; ++i)
    {
      var rawVal = rawDt.Val;
      ref var val = ref values.InnerArray[i];

      // group
      if (val.Unit == rawVal.Unit && val.Type == rawVal.Type)
      {
        // filter
        if (val.Ts == rawVal.Ts && val.Shift > rawVal.Shift)
        {
          val.Update(rawVal.Shift, rawVal.Rate, rawVal.Ts);
          break;
        }
      }
    }

    values.Add(rawDt.Val);
  }
  else
  {
    aggData.Add(rawDt.Key, new DynArr<ModelValV2>(40) { rawDt.Val });
  }
}
```

Вместо group by (под капотом look up), сами группируем через словарь.
Ключ с меньшим кол-вом параметров, но с перебором в for оказался быстрее.
Можно потратить больше памяти и сделать еще быстрее за O(1).
Фильтр потерял одну проверку (данные тянем в DESC) не нужно искать Max в группе.

</details>

<details>
<summary>calc model</summary>

```c#
public IEnumerable<object[]> CalcModelV2(Dictionary<ModelKey, DynArr<ModelValV2>> aggData, ModelConfig modelCfg, Area area)
{
  var specificCfg = modelCfg.Specific[area];
  var commonCfg = modelCfg.Common;

  foreach (var aggDt in aggData)
  {
    var commonKey = new CommonConfigKey(aggDt.Key.AppId);
    commonCfg.TryGetValue(commonKey, out var common);

    var shifts = new ushort[aggDt.Value.Count];

    for (var i = 0; i <  aggDt.Value.Count; ++i)
    {
      var aggVal = aggDt.Value[i];

      var specificKey = new SpecificConfigKey(aggVal.Unit);
      specificCfg.TryGetValue(specificKey, out var specific);

      var shift = CalcValueOrZero(common, specific, aggVal.Rate, aggVal.Shift);

      shifts[i] = shift;
    }

    yield return new object[]
    {
      aggDt.Key.BidId,
      aggDt.Key.AppId,
      aggDt.Key.Tags,
      aggDt.Value.Select(x => x.Type).ToArray(),
      aggDt.Value.Select(x => x.Unit).ToArray(),
      shifts,
    };
  }
}
```

В v1 версии использовали Join, словари. Если не нашли Value (практически не возможная ситуация, фильтры довольно статичны), получаем default и 0 в результате.

</details>

<details>
<summary>insert</summary>

```c#
public async Task InsertV3(IEnumerable<object[]> modelData, Area area, CancellationToken token = default)
{
  string[] columns = 
  {
    "AppId",
    "BidId",
    "Tags",
    "Types",
    "Units",
    "Shifts"
  };

  using (var conn = new ClickHouseConnection(_chStr))
  using (var bulk = new ClickHouseBulkCopy(conn)
  {
    DestinationTableName = $"model_{area}",
    BatchSize = 100_000
  })
  {
    await bulk.WriteToServerAsync(modelData, columns, token);
  }
}
```

Не нравится только Batch внутри записи с лишней аллокацией.

</details>

<details>
<summary>extra</summary>

Схема для трех area одновременно ``Dictionary<ModelKey, DynArr<ModelVal3>[3]>``.\
Даже если у вас одна таблица или она с партициями, но все на одном диске.
Вы можете сделать несколько читателей и это может ускорить процесс.\
Сервер БД и планировщик ОСи делят ресурсы между коннектами. Но если ресурсы машины позволяют, то открытие еще одного коннекта может дать прирост скорости.

Если у вас есть реплики или шарды, то можно смело параллелить чтение.

</details>

### B v3

```c#
public override async Task Run() // 2-4 min sum
{
  var modelConfig = _builder.GetModelConfig();
  var areas = new[] { Area.ZoneA, Area.Konoha, Area.ZoneC };
  var modelData = new List<object[]>(120_000);

  BoundedChannelOptions opt = new BoundedChannelOptions(10_000)
  {
    SingleReader = true,
    SingleWriter = true,
    FullMode = BoundedChannelFullMode.Wait
  };

  foreach (var area in areas)
  {
    var channel = Channel.CreateBounded<WrapV3>(opt);

    // select aggregated
    var readTask = Task.Run(async () =>
    {
      try
      {
        foreach (var aggItem in _rep.SelectV3(area, _cts.Token))
        {
          await channel.Writer.WriteAsync(aggItem, _cts.Token);
        }
      }
      finally
      {
        channel.Writer.TryComplete();
      }
    });

    // calc
    var calcTask = Task.Run(async () =>
    {
      while (await channel.Reader.WaitToReadAsync(_cts.Token))
      {
        var aggDt = await channel.Reader.ReadAsync(_cts.Token);
        var modelDt = _builder.CalcModelV3(aggDt, modelConfig, area);

        modelData.Add(modelDt);

        // insert
        if (modelData.Count == 100_000)
        {
          await _rep.InsertV3(modelData, area, _cts.Token);
          modelData.Clear();
        }
      }
    });

    await _rep.InsertV3(modelData, area, _cts.Token);
    modelData.Clear();

    await Task.WhenAll(readTask, calcTask);
  }
}
```

Сохранение не такое долгое, делаем это быстрее чем упремся в емкость канала.
Решение спорное. Клик любит вставки миллионными пачками, поэтому нет одиночной.

<details>
<summary>extra</summary>

На чтении применимы те же трюки, что с mysql.\
Но только на шардах, группировка в памяти 3 * 70М может оказаться затратной для одной машины.
Сжатие идет через gzip, и на этом спасибо, но он не такой быстрый как zstd при том же уровне или чуть лучшем уровне компрессии.

На сохранении возможный способ экономно и без аллокаций в object[] сохранить результаты для вставки это испоьзовать appache arrow. Но с ним не умеют рабоать ни драйвер, ни я.
Если у вас есть нормальные туториалы, поделитесь в комментариях.

</details>

<details>
<summary>select</summary>

```c#
public IEnumerable<WrapV3> SelectV3(Area area, CancellationToken token = default)
{
  var sql = $@"SELECT
AppId, 
BidId, 
Tags, 
Type, 
Units, 
Shifts, 
Rates 
FROM agg_{area} 
ORDER BY Id DESC";

  using (var conn = new ClickHouseConnection(_chStr))
  using (var cmd = conn.CreateCommand())
  {
    conn.Open();
    cmd.CommandText = sql;

    using (var reader = cmd.ExecuteReader())
    {
      while (reader.Read() || reader.NextResult() && reader.Read())
      {
        if (token.IsCancellationRequested)
          throw new OperationCanceledException();

        var i = -1;
        yield return new WrapV3 (
          key: new ModelKey (
            appId: reader.GetFieldValue<ushort>(++i),
            bidId: reader.GetFieldValue<ushort>(++i),
            tags: reader.GetFieldValue<string>(++i)),
          val: new ModelValV3 (
            types: reader.GetFieldValue<byte[]>(++i),
            units: reader.GetFieldValue<byte[]>(++i),
            shifts: reader.GetFieldValue<ushort[]>(++i)),
          rates: reader.GetFieldValue<int[]>(++i));
      }
    }
  }
}
```

</details>

<details>
<summary>calc model</summary>

```c#
public object[] CalcModelV3(WrapV3 aggDt, ModelConfig modelCfg, Area area)
{
  var commonCfg = modelCfg.Common;
  var specificCfg = modelCfg.Specific[area];

  var commonKey = new CommonConfigKey(aggDt.Key.AppId);
  commonCfg.TryGetValue(commonKey, out var common);

  for (var i = 0; i < aggDt.Val.Units.Length; ++i)
  {
    var specificKey = new SpecificConfigKey(aggDt.Val.Units[i]);
    specificCfg.TryGetValue(specificKey, out var specific);

    aggDt.Val.Shifts[i] = CalcValueOrZero(common, specific, aggDt.Rates[i], aggDt.Val.Shifts[i]);
  }

  return new object[]
  {
    aggDt.Key.BidId,
    aggDt.Key.AppId,
    aggDt.Key.Tags,
    aggDt.Val.Types,
    aggDt.Val.Units,
    aggDt.Val.Shifts,
  };
}
```

</details>

<details>
<summary>insert</summary>

Вставка такая же, как в v2.

</details>

## C, pack. code

Сразу приведу версию v3, потому что v1 для этой части еще хуже, чем B v1.
Это была бы пустая трата вашего трафика и пикселей на ваших экранах.

```C#
static RecyclableMemoryStreamManager _manager = new RecyclableMemoryStreamManager();

Channel<WrapV3> _channelReadZip;
Channel<RecyclableMemoryStream> _queueZipSave;

CancellationTokenSource _cts;

public async Task RunAsync()
{
  var areas = new[] { Area.ZoneA, Area.Konoha, Area.ZoneC };
  foreach (var area in areas)
  {
    var read = Task.Run(async () => await ReadAsync(area), _cts.Token);
    var proc = Task.Run(async () => await ProcAsync(), _cts.Token);
    var gzip = Task.Run(async () => await ZipAsync(), _cts.Token);
    var save = Task.Run(async () => await SaveAsync(area), _cts.Token);

    await Task.WhenAll(read, proc, gzip, save);
  }
}

private IEnumerable<WrapV3> SelectModel(Area area)
  => _rep.SelectV3(area, _cts.Token);

private Task ProcAsync() => Task.CompletedTask;
```

Чтение такое же, как для B v3, только таблица теперь ``"model_{area}"``

ProcAsync бывает нужен, когда схема модели и словарем отличается, например, меняем группировку. Или для сортировки значений.

```c#
private async Task ZipAsync()
{
  var readSem = new SemaphoreSlim(1);

  var tasks = new List<Task>();
  try
  {
    for (var i = 0; i < THREADS_ZIP; i++)
    {
      var task = Task.Run(async () =>
      {
        using (var bin = _manager.GetStream(StreamTag.TO_BINARY, BINARY_SIZE))
        {
          // читает только один поток
          await readSem.WaitAsync(_cts.Token);
          while (await _channelReadZip.Reader.WaitToReadAsync(_cts.Token))
          {
            // чтение и сериализация быстрая, поэтому в один поток (меньше синхронизаций)
            if (bin.Length < BINARY_SIZE - BINARY_PADDING)
            {
              var modelDt = await _channelReadZip.Reader.ReadAsync(_cts.Token);
              Serialize(bin, ref modelDt);
            }
            // сжатие долгое, отдаем семафор на чтение, сжимаем данные, ждем чтения
            else
            {
              readSem.Release();

              await ZipHelperAsync(bin);

              await readSem.WaitAsync(_cts.Token);
            }
          }

          readSem.Release();
          if (bin.Length > 0)
          {
            await ZipHelperAsync(bin);
          }
        }
      }, _cts.Token);
      tasks.Add(task);
    }

    await Task.WhenAll(tasks);
  }
  finally
  {
    readSem.Dispose();
    _queueZipSave.Writer.TryComplete();
  }
}

private void Serialize(MemoryStream bin, ref WrapV3 modelDt)
{
  var seekPrefix = bin.Position;
  bin.Seek(sizeof(int), SeekOrigin.Current); // add index space

  ZeroFormatterSerializer.Serialize(bin, modelDt);

  var seekCurrent = bin.Position;
  bin.Seek(seekPrefix, SeekOrigin.Begin); // set index to
  bin.WriteInt((int)(seekCurrent - seekPrefix - sizeof(int)));
  bin.Seek(seekCurrent, SeekOrigin.Begin);
}

private async Task ZipHelperAsync(MemoryStream bin)
{
  bin.Seek(0, SeekOrigin.Begin);
  var zip = _manager.GetStream(StreamTag.TO_ZIP, ZIP_SIZE);

  using (var opt = new CompressionOptions(compressionLevel: 1))
  using (var cmp = new CompressionStream(zip, opt))
  {
    await bin.CopyToAsync(cmp, bufferSize: 81920, _cts.Token);
  }

  // Прото стримы переиспользуем в рамках таски
  bin.Seek(0, SeekOrigin.Begin);
  bin.SetLength(0);

  zip.Seek(0, SeekOrigin.Begin);
  await _queueZipSave.Writer.WriteAsync(zip, _cts.Token);
}
```

Максимально переиспользуем стримы при сериализации и сжатии благодаря RecyclableMemoryStreamManager. bin стримы переиспользуем, чтобы лишний раз не бегать к менеджеру с запросом на новый. ZeroFomatter не самый умный, самостоятельно дописываем размеры для каждой сериалзованной структуры.

```c#
private async Task SaveBinaryAsync(Area area)
{
  var blobIndex = 0;
  var timestamp = DateTime.UtcNow;

  while (await _queueZipSave.Reader.WaitToReadAsync(_cts.Token))
  {
    var zipStream = await _queueZipSave.Reader.ReadAsync(_cts.Token);
    var blobName = $"{area}_{blobIndex}_{timestamp}";
    // todo: save blob to redis
    zipStream.Close();
  }
}
```

Получили zip стрим, отправили содержимое в Redis, и закрыли его, то есть отдали менеджеру.

## C, unpack. Code

Ообратный процесс - распаковка словаря.\
Упоминания заслуживают zeroFomatter и zstd.
Благодаря этим двум библиотекам скорость распаковки увеиличилась в 25-50 раз.

| Compressor name     | Ratio | Compression| Decompress.|
| ------------------- | ------| -----------| ---------- |
| zstd 1.5.1 -1       | 2.887 |   530 MB/s |  1700 MB/s |
| zlib 1.2.11 -1      | 2.743 |    85 MB/s |   400 MB/s |
| zstd 1.5.1 --fast=1 | 2.437 |   600 MB/s |  2150 MB/s |
| zstd 1.5.1 --fast=3 | 2.239 |   670 MB/s |  2250 MB/s |
| zstd 1.5.1 --fast=4 | 2.148 |   710 MB/s |  2300 MB/s |
| lz4 1.9.3           | 2.101 |   740 MB/s |  4500 MB/s |

C# gzip и zlib очень схожи, внутри у них DEFLATE.\
При том же уровне сжатия zstd почти на порядок быстрее zlib. На распаковке быстрее в 5 раз.
Можно еще попробовать lz4, но для тех данных zstd справлялся лучше.

## E, calc. Code

Смена ключа с [area, appId, bidId, tags, type] на [appId, bidId, tags]:

* ускорило поиск значения
* уменьшило потребление RAM
* разгрузило GC

Словарь порезался на чанки, плюс приобрел некоторые доп.методы для корректного обновления.
Базовая реализация очень схожа с тем, о чем говорят тут

[Avoid LOH Dictionary](https://habr.com/en/companies/jugru/articles/468611/#:~:text=%D0%BD%D0%B5%20%D0%BE%D1%87%D0%B5%D0%BD%D1%8C%20%D0%BA%D1%80%D0%B8%D1%82%D0%B8%D1%87%D0%BD%D0%BE.-,%D0%9C%D1%8B%20%D1%85%D0%BE%D1%82%D0%B8%D0%BC,-%2C%20%D1%87%D1%82%D0%BE%D0%B1%D1%8B%20%D0%BD%D0%B0%D1%88%D0%B0%20%D1%81%D1%82%D1%80%D1%83%D0%BA%D1%82%D1%83%D1%80%D0%B0)

