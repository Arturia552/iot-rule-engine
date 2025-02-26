use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;
use uuid::Uuid;
use env_logger;

// 实体类型枚举
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityType {
    Device,
    Asset,
    Customer,
    RuleChain,
    RuleNode,
    Tenant,
    User,
    Dashboard,
    Alarm,
}

// 实体ID结构
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityId {
    pub entity_type: EntityType,
    pub id: Uuid,
}

// 租户ID结构
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId {
    pub id: Uuid,
}

// 组件生命周期状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComponentLifecycleState {
    Active,
    Suspended,
    NotInitialized,
}

// 消息元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TbMsgMetaData {
    data: HashMap<String, String>,
}

impl TbMsgMetaData {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn put_value(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    pub fn get_value(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn copy(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

// 消息事务数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TbMsgTransactionData {
    pub is_new_transaction: bool,
    pub transaction_id: Option<Uuid>,
}

// 核心消息结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TbMsg {
    pub id: Uuid,
    pub msg_type: String,
    pub originator: EntityId,
    pub metadata: TbMsgMetaData,
    pub data: String,
    pub timestamp: i64,
    pub transaction_data: TbMsgTransactionData,
}

impl TbMsg {
    pub fn new(
        msg_type: String,
        originator: EntityId,
        metadata: TbMsgMetaData,
        data: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            msg_type,
            originator,
            metadata,
            data,
            timestamp: chrono::Utc::now().timestamp_millis(),
            transaction_data: TbMsgTransactionData {
                is_new_transaction: true,
                transaction_id: None,
            },
        }
    }

    pub fn copy(&self) -> Self {
        Self {
            id: Uuid::new_v4(),
            msg_type: self.msg_type.clone(),
            originator: self.originator.clone(),
            metadata: self.metadata.copy(),
            data: self.data.clone(),
            timestamp: self.timestamp,
            transaction_data: self.transaction_data.clone(),
        }
    }

    pub fn get_payload(&self) -> &str {
        &self.data
    }
}

// 节点配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleNodeConfiguration {
    pub config: JsonValue,
}

// 规则节点上下文接口
#[async_trait]
pub trait TbContext: Send + Sync {
    async fn tell_next(&self, msg: TbMsg, relation_type: &str) -> Result<(), String>;
    async fn tell_self(&self, msg: TbMsg, delay_ms: u64) -> Result<(), String>;
    async fn tell_failure(&self, msg: TbMsg, error: String) -> Result<(), String>;
    async fn get_self_id(&self) -> Uuid;
    async fn get_tenant_id(&self) -> TenantId;
    async fn get_service_by_id(&self, service_type: &str) -> Option<Arc<dyn Any + Send + Sync>>;
}

// 规则节点接口
#[async_trait]
pub trait TbNode: Send + Sync {
    async fn init(
        &mut self,
        ctx: Arc<dyn TbContext>,
        config: RuleNodeConfiguration,
    ) -> Result<(), String>;
    async fn on_msg(&self, ctx: Arc<dyn TbContext>, msg: TbMsg) -> Result<(), String>;
    async fn destroy(&self) -> Result<(), String>;
}

// 节点连接信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConnectionInfo {
    pub from_index: usize,
    pub to_index: usize,
    pub relation_type: String,
}

// 规则链连接信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleChainConnectionInfo {
    pub from_index: usize,
    pub target_rule_chain_id: Uuid,
    pub relation_type: String,
}

// 规则节点结构
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RuleNode {
    pub id: Uuid,
    pub rule_chain_id: Uuid,
    pub node_type: String,
    pub name: String,
    pub debug_mode: bool,
    pub configuration_version: i32,
    pub configuration: JsonValue,
    pub additional_info: Option<JsonValue>,
}

// 规则链结构
#[derive(Debug, Serialize, Deserialize)]
pub struct RuleChain {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub name: String,
    pub first_rule_node_id: Option<Uuid>,
    pub root: bool,
    pub debug_mode: bool,
    pub configuration: Option<JsonValue>,
}

// 规则链元数据
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RuleChainMetaData {
    pub rule_chain_id: Uuid,
    pub first_node_index: Option<usize>,
    pub nodes: Vec<RuleNode>,
    pub connections: Vec<NodeConnectionInfo>,
}

use std::any::Any;
use std::time::Duration;

// 规则节点上下文实现
#[derive(Clone)]
pub struct DefaultTbContext {
    tenant_id: TenantId,
    rule_chain_id: Uuid,
    rule_node_id: Uuid,
    msg_sender: Sender<RuleEngineMsg>,
    services: Arc<Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>>,
}

impl DefaultTbContext {
    pub fn new(
        tenant_id: TenantId,
        rule_chain_id: Uuid,
        rule_node_id: Uuid,
        msg_sender: Sender<RuleEngineMsg>,
    ) -> Self {
        Self {
            tenant_id,
            rule_chain_id,
            rule_node_id,
            msg_sender,
            services: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn register_service(
        &self,
        service_type: String,
        service: Arc<dyn Any + Send + Sync>,
    ) {
        let mut services = self.services.lock().await;
        services.insert(service_type, service);
    }
}

#[async_trait]
impl TbContext for DefaultTbContext {
    async fn tell_next(&self, msg: TbMsg, relation_type: &str) -> Result<(), String> {
        let engine_msg = RuleEngineMsg::NodeToNodeMsg {
            from_id: self.rule_node_id,
            msg,
            relation_type: relation_type.to_string(),
        };
        self.msg_sender
            .send(engine_msg)
            .await
            .map_err(|e| e.to_string())
    }

    async fn tell_self(&self, msg: TbMsg, delay_ms: u64) -> Result<(), String> {
        let engine_msg = RuleEngineMsg::SelfMsg {
            node_id: self.rule_node_id,
            msg,
            delay_ms,
        };
        self.msg_sender
            .send(engine_msg)
            .await
            .map_err(|e| e.to_string())
    }

    async fn tell_failure(&self, msg: TbMsg, error: String) -> Result<(), String> {
        let engine_msg = RuleEngineMsg::FailureMsg {
            node_id: self.rule_node_id,
            msg,
            error,
        };
        self.msg_sender
            .send(engine_msg)
            .await
            .map_err(|e| e.to_string())
    }

    async fn get_self_id(&self) -> Uuid {
        self.rule_node_id
    }

    async fn get_tenant_id(&self) -> TenantId {
        self.tenant_id.clone()
    }

    async fn get_service_by_id(&self, service_type: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        let services = self.services.lock().await;
        services.get(service_type).cloned()
    }
}

// 规则引擎消息类型
#[derive(Debug)]
pub enum RuleEngineMsg {
    NodeToNodeMsg {
        from_id: Uuid,
        msg: TbMsg,
        relation_type: String,
    },
    ChainToChainMsg {
        from_chain_id: Uuid,
        to_chain_id: Uuid,
        msg: TbMsg,
    },
    SelfMsg {
        node_id: Uuid,
        msg: TbMsg,
        delay_ms: u64,
    },
    FailureMsg {
        node_id: Uuid,
        msg: TbMsg,
        error: String,
    },
    InitMsg {
        chain_id: Uuid,
        metadata: RuleChainMetaData,
    },
    StopMsg {
        chain_id: Uuid,
    },
}

// 规则节点上下文和实例
struct RuleNodeCtx {
    node: Box<dyn TbNode>,
    ctx: Arc<DefaultTbContext>,
}

// 规则引擎执行器
pub struct RuleEngineActorMessageProcessor {
    tenant_id: TenantId,
    rule_chains: HashMap<Uuid, RuleChain>,
    rule_chain_metadata: HashMap<Uuid, RuleChainMetaData>,
    node_actors: HashMap<Uuid, RuleNodeCtx>,
    node_relations: HashMap<(Uuid, String), Vec<Uuid>>,
    chain_relations: HashMap<(Uuid, String), Vec<Uuid>>,
    msg_sender: Sender<RuleEngineMsg>,
    node_factory: Arc<dyn RuleNodeFactory>,
}

// 规则节点工厂接口
#[async_trait]
pub trait RuleNodeFactory: Send + Sync {
    async fn create_node(&self, node_type: &str) -> Result<Box<dyn TbNode>, String>;
}

impl RuleEngineActorMessageProcessor {
    pub fn new(
        tenant_id: TenantId,
        msg_sender: Sender<RuleEngineMsg>,
        node_factory: Arc<dyn RuleNodeFactory>,
    ) -> Self {
        Self {
            tenant_id,
            rule_chains: HashMap::new(),
            rule_chain_metadata: HashMap::new(),
            node_actors: HashMap::new(),
            node_relations: HashMap::new(),
            chain_relations: HashMap::new(),
            msg_sender,
            node_factory,
        }
    }

    pub async fn init_rule_chain(
        &mut self,
        chain_id: Uuid,
        metadata: RuleChainMetaData,
    ) -> Result<(), String> {
        // 存储规则链元数据
        self.rule_chain_metadata.insert(chain_id, metadata.clone());

        // 清除先前的节点关系
        self.node_relations.retain(|(id, _), _| *id != chain_id);

        // 初始化节点
        for node in &metadata.nodes {
            let mut node_instance = self.node_factory.create_node(&node.node_type).await?;
            let ctx = Arc::new(DefaultTbContext::new(
                self.tenant_id.clone(),
                chain_id,
                node.id,
                self.msg_sender.clone(),
            ));

            let config = RuleNodeConfiguration {
                config: node.configuration.clone(),
            };

            node_instance.init(ctx.clone(), config).await?;

            self.node_actors.insert(
                node.id,
                RuleNodeCtx {
                    node: node_instance,
                    ctx,
                },
            );
        }
        // 构建节点关系
        for conn in &metadata.connections {
            let from_node = &metadata.nodes[conn.from_index];
            let to_node = &metadata.nodes[conn.to_index];

            let key = (from_node.id, conn.relation_type.clone());
            let entry = self.node_relations.entry(key).or_insert_with(Vec::new);
            if !entry.contains(&to_node.id) {
                entry.push(to_node.id);
            }
        }
        println!("{:?}", self.node_relations);
        Ok(())
    }

    pub async fn process_msg(&self, msg: RuleEngineMsg) -> Result<(), String> {
        match msg {
            RuleEngineMsg::NodeToNodeMsg {
                from_id,
                msg,
                relation_type,
            } => {
                
                let key = (from_id, relation_type);
                if let Some(next_nodes) = self.node_relations.get(&key) {
                    for &next_id in next_nodes {
                        if let Some(node_ctx) = self.node_actors.get(&next_id) {
                            let msg_copy = msg.copy();
                            node_ctx.node.on_msg(node_ctx.ctx.clone(), msg_copy).await?;
                        }
                    }
                }

                // 检查是否有链间连接
                if let Some(next_chains) = self.chain_relations.get(&key) {
                    for &next_chain_id in next_chains {
                        let chain_msg = RuleEngineMsg::ChainToChainMsg {
                            from_chain_id: self.get_chain_id_for_node(from_id)?,
                            to_chain_id: next_chain_id,
                            msg: msg.copy(),
                        };
                        self.msg_sender
                            .send(chain_msg)
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                }
            }
            RuleEngineMsg::ChainToChainMsg {
                to_chain_id, msg, ..
            } => {
                // 将消息发送到目标规则链的第一个节点
                if let Some(metadata) = self.rule_chain_metadata.get(&to_chain_id) {
                    if let Some(first_idx) = metadata.first_node_index {
                        let first_node = &metadata.nodes[first_idx];
                        if let Some(node_ctx) = self.node_actors.get(&first_node.id) {
                            node_ctx.node.on_msg(node_ctx.ctx.clone(), msg).await?;
                        }
                    }
                }
            }
            RuleEngineMsg::SelfMsg { node_id, msg, .. } => {
                if let Some(node_ctx) = self.node_actors.get(&node_id) {
                    node_ctx.node.on_msg(node_ctx.ctx.clone(), msg).await?;
                }
            }
            RuleEngineMsg::FailureMsg { .. } => {
                // 处理失败消息的逻辑
                // 可以记录日志或者尝试其他备用路径
            }
            _ => {
                // 其他消息类型的处理
            }
        }

        Ok(())
    }

    fn get_chain_id_for_node(&self, node_id: Uuid) -> Result<Uuid, String> {
        for (chain_id, metadata) in &self.rule_chain_metadata {
            for node in &metadata.nodes {
                if node.id == node_id {
                    return Ok(*chain_id);
                }
            }
        }
        Err("Node not found in any rule chain".to_string())
    }
}

// 消息队列Actor
pub struct MsgQueueActor {
    receiver: Receiver<RuleEngineMsg>,
    processor: Arc<Mutex<RuleEngineActorMessageProcessor>>,
}

impl MsgQueueActor {
    pub fn new(
        receiver: Receiver<RuleEngineMsg>,
        processor: Arc<Mutex<RuleEngineActorMessageProcessor>>,
    ) -> Self {
        Self {
            receiver,
            processor,
        }
    }

    pub async fn run(&mut self) -> Result<(), String> {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                RuleEngineMsg::InitMsg { chain_id, metadata } => {
                    let mut processor = self.processor.lock().await;
                    processor.init_rule_chain(chain_id, metadata).await?;
                }
                RuleEngineMsg::StopMsg { chain_id } => {
                    // 停止规则链的逻辑
                },
                _ => {
                    let processor = self.processor.lock().await;
                    processor.process_msg(msg).await?;
                }
            }
        }

        Ok(())
    }
}

// 一个简单的过滤器节点实现示例
pub struct FilterNode {
    key: String,
    value: String,
}

impl FilterNode {
    pub fn new() -> Self {
        Self {
            key: String::new(),
            value: String::new(),
        }
    }
}

#[async_trait]
impl TbNode for FilterNode {
    async fn init(
        &mut self,
        _ctx: Arc<dyn TbContext>,
        config: RuleNodeConfiguration,
    ) -> Result<(), String> {
        if let Some(key) = config.config.get("key") {
            self.key = key.as_str().unwrap_or("").to_string();
        }

        if let Some(value) = config.config.get("value") {
            self.value = value.as_str().unwrap_or("").to_string();
        }

        Ok(())
    }

    async fn on_msg(&self, ctx: Arc<dyn TbContext>, msg: TbMsg) -> Result<(), String> {
        // 检查元数据中是否包含指定的键和值
        if let Some(meta_value) = msg.metadata.get_value(&self.key) {
            if meta_value == &self.value {
                // 条件匹配，将消息传递到"True"关系
                ctx.tell_next(msg, "True").await
            } else {
                // 条件不匹配，将消息传递到"False"关系
                ctx.tell_next(msg, "False").await
            }
        } else {
            // 键不存在，将消息传递到"False"关系
            ctx.tell_next(msg, "False").await
        }
    }

    async fn destroy(&self) -> Result<(), String> {
        // 清理资源的逻辑
        Ok(())
    }
}

// 一个简单的规则节点工厂实现
pub struct SimpleRuleNodeFactory;

#[async_trait]
impl RuleNodeFactory for SimpleRuleNodeFactory {
    async fn create_node(&self, node_type: &str) -> Result<Box<dyn TbNode>, String> {
        match node_type {
            "FILTER" => Ok(Box::new(FilterNode::new())),
            // 更多节点类型的实现...
            _ => Err(format!("Unsupported node type: {}", node_type)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    env_logger::init();
    println!("规则引擎启动中...");

    // 创建消息通道
    let (sender, receiver) = mpsc::channel::<RuleEngineMsg>(100);

    // 创建租户ID
    let tenant_id = TenantId {
        id: Uuid::new_v4(),
    };

    // 创建规则节点工厂
    let node_factory = Arc::new(SimpleRuleNodeFactory);

    // 创建规则引擎处理器
    let processor = Arc::new(Mutex::new(RuleEngineActorMessageProcessor::new(
        tenant_id.clone(),
        sender.clone(),
        node_factory,
    )));

    // 创建并启动消息队列Actor
    let mut msg_queue_actor = MsgQueueActor::new(receiver, processor.clone());
    let msg_queue_handle = tokio::spawn(async move {
        if let Err(e) = msg_queue_actor.run().await {
            eprintln!("Message queue actor error: {}", e);
        }
    });

    // 创建规则链
    let rule_chain_id = Uuid::new_v4();
    println!("创建规则链: {}", rule_chain_id);

    // 创建两个过滤器节点
    let node1_id = Uuid::new_v4();
    let node2_id = Uuid::new_v4();
    let node3_id = Uuid::new_v4();
    println!("创建过滤器节点1: {}", node1_id);
    println!("创建过滤器节点2: {}", node2_id);
    println!("创建过滤器节点3: {}", node3_id);

    // 构建规则链元数据
    let metadata = RuleChainMetaData {
        rule_chain_id,
        first_node_index: Some(0), // 第一个节点的索引
        nodes: vec![
            RuleNode {
                id: node1_id,
                rule_chain_id,
                node_type: "FILTER".to_string(),
                name: "温度过滤器".to_string(),
                debug_mode: true,
                configuration_version: 1,
                configuration: json!({
                    "key": "temperature",
                    "value": "high"
                }),
                additional_info: None,
            },
            RuleNode {
                id: node2_id,
                rule_chain_id,
                node_type: "FILTER".to_string(),
                name: "湿度过滤器".to_string(),
                debug_mode: true,
                configuration_version: 1,
                configuration: json!({
                    "key": "humidity",
                    "value": "high"
                }),
                additional_info: None,
            },
            RuleNode {
                id: node3_id,
                rule_chain_id,
                node_type: "FILTER".to_string(),
                name: "压力过滤器".to_string(),
                debug_mode: true,
                configuration_version: 1,
                configuration: json!({
                    "key": "pressure",
                    "value": "normal"
                }),
                additional_info: None,
            },
        ],
        connections: vec![
            // 连接两个节点：如果第一个过滤器返回True，连接到第二个节点
            NodeConnectionInfo {
                from_index: 0,
                to_index: 1,
                relation_type: "True".to_string(),
            },
            NodeConnectionInfo {
                from_index: 1,
                to_index: 2,
                relation_type: "True".to_string(),
            },
        ],
    };

    // 发送初始化消息
    sender
        .send(RuleEngineMsg::InitMsg {
            chain_id: rule_chain_id,
            metadata: metadata.clone(),
        })
        .await?;

    // 等待规则链初始化完成
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 创建设备实体ID
    let device_id = EntityId {
        entity_type: EntityType::Device,
        id: Uuid::new_v4(),
    };

    // 创建消息元数据
    let mut metadata1 = TbMsgMetaData::new();
    metadata1.put_value("temperature".to_string(), "high".to_string());
    metadata1.put_value("humidity".to_string(), "high".to_string());

    // 创建将通过规则链的消息
    let msg1 = TbMsg::new(
        "TELEMETRY".to_string(),
        device_id.clone(),
        metadata1,
        json!({"temp": 45, "humidity": 80}).to_string(),
    );

    println!("发送消息1: 高温度和高湿度");
    // 假设第一个节点是规则链的入口点
    let _node1_ctx = {
        let processor = processor.lock().await;
        processor.node_actors.get(&node1_id).unwrap().ctx.clone()
    };

    // 发送消息到第一个节点
    let _ = sender
        .send(RuleEngineMsg::SelfMsg {
            node_id: node1_id,
            msg: msg1,
            delay_ms: 0,
        })
        .await?;

    // 创建另一个消息，这个消息应该被第一个过滤器拦截
    let mut metadata2 = TbMsgMetaData::new();
    metadata2.put_value("temperature".to_string(), "low".to_string());
    metadata2.put_value("humidity".to_string(), "high".to_string());

    let msg2 = TbMsg::new(
        "TELEMETRY".to_string(),
        device_id.clone(),
        metadata2,
        json!({"temp": 20, "humidity": 80}).to_string(),
    );

    println!("发送消息2: 低温度和高湿度");
    // 发送第二个消息
    let _ = sender
        .send(RuleEngineMsg::SelfMsg {
            node_id: node1_id,
            msg: msg2,
            delay_ms: 0,
        })
        .await?;

    // 等待消息处理完成
    tokio::time::sleep(Duration::from_secs(100)).await;

    println!("规则引擎示例执行完成");
    Ok(())
}