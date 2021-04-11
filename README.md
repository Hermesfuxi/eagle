# eagle - 鹰
基于flink的电商实时数据分析、运营、推荐、风控项目

# 项目背景

平台运营到一定阶段，一定会累积大批量的用户数据，如何利用用户的数据来做运营（消息推送、触达消息、优惠券发送、广告位等），正是精准运营系统需要解决的问题。

一套实时风控+实时分析系统（仿阿里的实时风控），简单来说，就是一个基于事件驱动且可进行动态规则计算的实时系统；在技术上，它是通用的；本套架构及系统内核，不仅可以用于“实时运营”，也可以用于“实时风控”，“实时推荐”，“实时交通监控”等场景。

技术重点是要能在作业运行的时候去添加和删除规则，而不会因停止和重新启动作业从而造成高昂的代价。

## 业务场景

先看几个具有代表性的需求：

> 用户可用额度在20000～50000元，而且有借款记录，未还本金为0，性别为“男”
> 用户发生了A行为且未还本金大于5000
> 用户在1天内发生A行为次数大于等于3次
> 用户在A行为前24小时内未发生B行为
> 用户在A行为后一个月内未发生B行为

业务上有两种消息类型

- 日常消息：由业务人员通过条件筛选锁定用户群，定时或即时给批量用户发送消息或者优惠券<br />
- 触达消息：主要由用户自身的行为触发，比如登陆、进件申请、还款等，满足一定筛选条件**实时**给用户发送消息或优惠券<br />

对于用户筛选条件，也主要有两种类型

- 用户状态：包括用户自身属性如性别、年龄、学历、收入等，还有用户相关联实体如进件订单、账户信息、还款计划、优惠券等的属性，以及用户画像数据如行为偏好、进件概率等<br />
- 用户行为：即用户的动作，包括登陆、进件申请、还款，甚至前端点击某个按钮、在某个文本框输入都算<br />

## 系统搭建的目标

- 需要定义规则，提供可视化界面给业务人员动态配置，无需重启系统即使生效，减少沟通成本和避免重复开发，总之就是要更加 **自动化** 和 **易配置**<br />
- 采集实时数据，根据实时事件做**实时**推送

## 架构图
系统包含三个主要的组件：
• 前端（React）
• 后端（SpringBoot）
• 欺诈检测 Flink 应用程序
三者之间的组成关系如下图所示：
![image](https://cdn.nlark.com/yuque/0/2021/jpeg/181105/1617692261451-d544c124-9569-4d7b-8ce2-9ec075099679.jpeg?x-oss-process=image%2Fresize%2Cw_2688)
后端将 REST API 暴露给前端，用于创建/删除规则以及发出用于管理演示执行的控制命令，然后，它会将这些前端操作行为数据发送到 Kafka Topic Control 中。后端还包含了一个交易数据生成器组件，该组件用来模拟交易数据的，然后会将这些交易数据发送到 Kafka Topic Transactions 中，这些数据最后都会被 Flink 应用程序去消费，Flink 程序经过规则计算这些交易数据后生成的告警数据会发送到 Kafka Topic Alerts 中，并通过 Web Sockets 将数据传到前端 UI。

## 技术选型

项目本身是基于Flink+ClickHouse的Lambda架构，使用drools规则引擎，基于Spring boot+Vue构建规则的管理系统（还在构建中，支持规则、模板、策略、黑白名单等的增删改查），并能基于模板引擎Beetl生成动态SQL，并存储到Mysql中，由canal 监听到Mysql的binlog 后加载到Kafka，再由Kafka流入Flink和ClickHouse，Flink做用户行为的实时计算，ClickHouse做离线计算，支持动态数据分区与规则配置（Flink广播流），支持类与Jar文件的动态编译与动态加载，利用ProcessFunction复杂的自定义逻辑来“模拟”窗口，redis做缓存，HBase存储用户画像数据（模拟生成，后续会建立实时画像模块），后期打算接入机器学习-专家系统等模块，项目现阶段仍处于构建阶段，文档也在补充当中。

下面重点看下kafka connector

### kafka connector

kafka connector有Source和Sink两种组件，Source的作用是读取数据到kafka，这里用开源实现debezium来采集mysql的binlog和postgres的xlog。Sink的作用是从kafka读数据写到目标系统，这里自己研发一套组件，根据配置的规则将数据格式化再同步到ES。<br />kafka connector有以下优点：

- 提供大量开箱即用的插件，比如我们直接用debezium就能解决读取mysql和pg数据变更的问题<br />
- 伸缩性强，对于不同的connector可以配置不同数量的task，分配给不同的worker，，我们可以根据不同topic的流量大小来调节配置。<br />
- 容错性强，worker失败会把task迁移到其它worker上面<br />
- 使用rest接口进行配置，我们可以对其进行包装很方便地实现一套管理界面<br />

示例规则定义：<br />
![](https://cdn.nlark.com/yuque/0/2021/jpeg/181105/1617694584837-7f9b9172-b4c4-4f37-96e2-512f3bfd3ae4.jpeg#align=left&display=inline&height=572&margin=%5Bobject%20Object%5D&originHeight=572&originWidth=2302&size=0&status=done&style=none&width=2302)

### 数据动态分区 DynamicKeyFunction
<br />下面介绍使用 `DynamicKeyFunction`提取数据含 `groupingKeyNames` 里面字段组成数据分组 key 的方法
<br />一般在程序中，数据分区的 keyBy 字段是固定的，由数据内的某些静态字段确定，例如，当构建一个简单的基于窗口的交易流聚合时，我们可能总是按照交易账户 ID 进行分组。<br />

```
DataStream<Transaction> input = // [...]
DataStream<...> windowed = input
  .keyBy(Transaction::getAccountId)
  .window(/*window specification*/);
```

<br />这种方法是在广泛的用例中实现水平可伸缩性的主要模块，但是在应用程序试图在运行时提供业务逻辑灵活性的情况下，这还是不够的。
<br />以个现实的样本规则定义为例：

> 在一个星期 之内，当 用户 A 累计 向 B 用户支付的金额超过 1000000 美元，则触发一条告警

PS：A 和 B 用字段描述的话分别是 付款人（payer）和受益人（beneficiary）<br />
<br />在上面的规则中，可以发现许多参数，我们希望能够在新提交的规则中指定这些参数，甚至可能在运行时进行动态的修改或调整：

- 聚合的字段（付款金额）
- 分组字段（付款人和受益人）
- 聚合函数（求和）
- 窗口大小（1 星期）
- 阈值（1000000）
- 计算符号（大于）

<br />因此，我们将使用以下简单的 JSON 格式来定义上述参数：

```
{
  "ruleId": 1,
  "ruleState": "ACTIVE",
  "groupingKeyNames": ["payerId", "beneficiaryId"],
  "aggregateFieldName": "paymentAmount",
  "aggregatorFunctionType": "SUM",
  "limitOperatorType": "GREATER",
  "limit": 1000000,
  "windowMinutes": 10080
}
```

<br />在这一点上，重要的是了解 groupingKeyNames 决定了数据的实际物理分区，所有指定参数（payerId + beneficiaryId）相同的交易数据都会汇总到同一个物理计算 operator 里面去。<br />
<br />而Flink中的 keyBy() 函数大多数情况都是使用硬编码的 KeySelector，它提取特定数据的字段。但是，为了支持所需的灵活性，这里必须根据规则中的规范以更加动态的方式提取它们，为此，使用一个额外的运算符用于将每条数据分配到正确的聚合实例中。<br />
<br />总体而言，我们的主要处理流程如下所示：

```
DataStream<Alert> alerts =
    transactions
        .process(new DynamicKeyFunction())
        .keyBy(/* some key selector */);
        .process(/* actual calculations and alerting */)
```

<br />先前我们已经建立了每个规则定义一个 `groupingKeyNames` 参数，该参数指定将哪些字段组合用于传入事件的分组。每个规则可以使用这些字段的任意组合。同时，每个传入事件都可能需要根据多个规则进行评估。这意味着事件可能需要同时出现在计算 `operator` 的多个并行实例中，这些实例对应于不同的规则，因此需要进行分叉。确保此类事件的调度能达到 `DynamicKeyFunction()` 的目的。<br />
`DynamicKeyFunction`迭代一组已定义的规则，并通过 `keyBy()` 函数提取所有数据所需的分组 key ：
```
public class DynamicKeyFunction
    extends ProcessFunction<Transaction, Keyed<Transaction, String, Integer>> {
   ...
  /* Simplified */
  List<Rule> rules = /* Rules that are initialized somehow.
                        Details will be discussed in a future blog post. */;

  @Override
  public void processElement(
      Transaction event,
      Context ctx,
      Collector<Keyed<Transaction, String, Integer>> out) {

      for (Rule rule :rules) {
       out.collect(
           new Keyed<>(
               event,
               KeysExtractor.getKey(rule.getGroupingKeyNames(), event),
               rule.getRuleId()));
      }
  }
  ...
}
```

`KeysExtractor.getKey()`使用反射从数据中提取`groupingKeyNames`里面所有所需字段的值，并将它们拼接为字符串，例如`"{payerId=25;beneficiaryId=12}"`。Flink 将计算该字符串的哈希值，并将此特定组合的数据处理分配给集群中的特定服务器。这样就会跟踪付款人25和受益人12之间的所有交易，并在所需的时间范围内评估定义的规则。<br />
<br />注意，Keyed引入了具有以下签名的包装器类作为输出类型`DynamicKeyFunction`：

```
public class Keyed<IN, KEY, ID> {
  private IN wrapped;
  private KEY key;
  private ID id;
  ...
  public KEY getKey(){
      return key;
  }
}
```

此 POJO 的字段携带了以下信息：`wrapped`是原始数据，`key`是使用 `KeysExtractor`提取出来的结果，`id`是导致事件的调度规则的 ID（根据规则特定的分组逻辑）。<br />
<br />这种类型的事件将作为`keyBy()`函数的输入，并允许使用简单的 lambda 表达式作为KeySelector实现动态数据 shuffle 的最后一步。<br />

```
DataStream<Alert> alerts =
    transactions
        .process(new DynamicKeyFunction())
        .keyBy((keyed) -> keyed.getKey());
        .process(new DynamicAlertFunction())
```

通过`DynamicKeyFunction` ，事件被隐式复制，且在 Flink 集群中并行的执行每个规则评估。这样我们就获得了一个重要的功能——规则处理的水平可伸缩性。通过向集群添加更多服务器，即增加并行度，系统将能够处理更多规则。实现此功能的代价是数据重复，这可能会成为一个问题，具体取决于一组特定的参数，例如传入数据速率，可用网络带宽，事件有效负载大小等。在实际情况下，可以进行其他优化应用，例如组合计算具有相同 `groupingKeyNames` 的规则，或使用过滤层，将事件中不需要处理特定规则的所有字段删除。<br />

### 数据动态报警：Dynamic Alert Function
下面介绍规则第二部分中的参数由 `DynamicAlertFunction` 使用

定义了所执行操作的实际逻辑及其参数（例如告警触发限制）。这意味着相同的规则必须同时存在于`DynamicKeyFunction`和`DynamicAlertFunction`。<br />
<br />下图展示了系统的最终工作图：
<br /> ![](https://cdn.nlark.com/yuque/0/2021/jpeg/181105/1617694659107-19a3881c-d6c9-498e-b922-c2960c3153d9.jpeg#align=left&display=inline&height=1062&margin=%5Bobject%20Object%5D&originHeight=1062&originWidth=3074&size=0&status=done&style=none&width=3074)
<br />上图的主要模块是：
- `Transaction Source`：Flink 作业的 Source 端，它会并行的消费 Kafka 中的金融交易流数据
- **`Dynamic Key Function`：动态的提取数据分区的 key。随后的`keyBy`函数会将动态的 key 值进行 hash，并在后续运算符的所有并行实例之间相应地对数据进行分区。
- `Dynamic Alert Function`：累积窗口中的数据，并基于该窗口创建告警。

这里我们使用广播流来控制规则数据的动态加载，并将其连接到主数据流：

```
// Streams setup
DataStream<Transaction> transactions = [...]
DataStream<Rule> rulesUpdateStream = [...]

BroadcastStream<Rule> rulesStream = rulesUpdateStream.broadcast(RULES_STATE_DESCRIPTOR);

// Processing pipeline setup
 DataStream<Alert> alerts =
     transactions
         .connect(rulesStream)
         .process(new DynamicKeyFunction())
         .keyBy((keyed) -> keyed.getKey())
         .connect(rulesStream)
         .process(new DynamicAlertFunction())
```

可以通过调用`broadcast`方法并指定状态描述符，从任何常规流中创建广播流。在处理主数据流的事件时需要存储和查找广播的数据，因此，Flink 始终根据此状态描述符自动创建相应的广播状态。
<br />另请注意，广播状态始终是 KV 格式（`MapState`）。

```
public static final MapStateDescriptor<Integer, Rule> RULES_STATE_DESCRIPTOR =
        new MapStateDescriptor<>("rules", Integer.class, Rule.class);
```

连接`rulesStream`后会导致 `ProcessFunction` 的内部发生某些变化。也就是说`DynamicKeyFunction`实际上应该是一个`BroadcastProcessFunction`。

```java
public abstract class BroadcastProcessFunction<IN1, IN2, OUT> {

    public abstract void processElement(IN1 value,
                                        ReadOnlyContext ctx,
                                        Collector<OUT> out) throws Exception;

    public abstract void processBroadcastElement(IN2 value,
                                                 Context ctx,
                                                 Collector<OUT> out) throws Exception;

}
```

不同的是，添加 `processBroadcastElement`了方法，该方法是用于处理到达的广播规则流。以下新版本的`DynamicKeyFunction` 函数允许在 `processElement` 方法里面中动态的修改数据分发的 key 列表：

```java
public class DynamicKeyFunction
    extends BroadcastProcessFunction<Transaction, Rule, Keyed<Transaction, String, Integer>> {


  @Override
  public void processBroadcastElement(Rule rule,
                                     Context ctx,
                                     Collector<Keyed<Transaction, String, Integer>> out) {
    BroadcastState<Integer, Rule> broadcastState = ctx.getBroadcastState(RULES_STATE_DESCRIPTOR);
    broadcastState.put(rule.getRuleId(), rule);
  }

  @Override
  public void processElement(Transaction event,
                           ReadOnlyContext ctx,
                           Collector<Keyed<Transaction, String, Integer>> out){
    ReadOnlyBroadcastState<Integer, Rule> rulesState =
                                  ctx.getBroadcastState(RULES_STATE_DESCRIPTOR);
    for (Map.Entry<Integer, Rule> entry : rulesState.immutableEntries()) {
        final Rule rule = entry.getValue();
        out.collect(
          new Keyed<>(
            event, KeysExtractor.getKey(rule.getGroupingKeyNames(), event), rule.getRuleId()));
    }
  }
}
```

在上面的代码中，`processElement()`接收事件流数据，并在 `processBroadcastElement()` 接收规则更新数据。创建新规则时，将如上面广播流的那张图所示进行分配，并会保存在所有使用 `processBroadcastState` 运算符的并行实例中。我们使用规则的 ID 作为存储和引用单个规则的 key。我们遍历动态更新的广播状态中的数据，而不是遍历硬编码的 `List<Rules>` 。<br />
<br />在将规则存储在广播 `MapState` 中时，`DynamicAlertFunction` 遵循相同的逻辑。如第 1 部分中所述，通过`processElement `方法输入的每条消息均应按照一个特定规则进行处理，并通过 `DynamicKeyFunction` 对其进行“预标记”并带有相应的ID。我们需要做的就是使用提供的 ID 从 `BroadcastState` 中检索相应规则，并根据该规则所需的逻辑对其进行处理。在此阶段，我们还将消息添加到内部函数状态，以便在所需的数据时间窗口上执行计算。<br />

### 计算逻辑
TODO 