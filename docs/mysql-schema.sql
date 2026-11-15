-- Marketing rules storage for MySQL 8 (user_behavior.marketing_rule).
-- Credentials are NOT stored here: the job reads MYSQL_PASSWORD environment
-- variable (or --mysql.password=... command line override).
-- Apply with:  mysql -uroot -p < docs/mysql-schema.sql

CREATE DATABASE IF NOT EXISTS user_behavior
    DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE user_behavior;

CREATE TABLE IF NOT EXISTS marketing_rule (
    id                 VARCHAR(64)  NOT NULL COMMENT '规则 ID，主键',
    name               VARCHAR(128) NOT NULL COMMENT '规则名称',
    pattern_name       VARCHAR(64)  NOT NULL COMMENT '匹配的行为模式，见 CepPatterns，* 为通配',
    coupon_template_id VARCHAR(64)  NOT NULL COMMENT '优惠券模板 ID',
    coupon_type        VARCHAR(16)  NOT NULL COMMENT '券类型：CASH/DISCOUNT/...',
    cooldown_seconds   BIGINT       NOT NULL DEFAULT 0 COMMENT '同一用户同一规则的触达冷却秒数',
    audience_levels    JSON         NULL     COMMENT '目标人群等级数组，如 ["GOLD","PLATINUM"]；NULL=全人群',
    enabled            TINYINT(1)   NOT NULL DEFAULT 1 COMMENT '是否启用',
    priority           INT          NOT NULL DEFAULT 0 COMMENT '多条命中时取最高',
    updated_at         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE = InnoDB COMMENT ='实时营销规则表（MysqlRuleRepository 增删改查）';

-- Seed rules, mirroring DefaultRules.initialRules()
INSERT INTO marketing_rule
    (id, name, pattern_name, coupon_template_id, coupon_type, cooldown_seconds, audience_levels, enabled, priority)
VALUES
    ('rule-cart-abandon',   '购物车未支付挽回', 'CART_NO_PAY_TIMEOUT', 'CPN_CART_10',   'CASH',     3600, NULL, 1, 100),
    ('rule-view-cart-order','下单后复购激励',   'VIEW_CART_ORDER',     'CPN_ORDER_20',  'DISCOUNT', 7200, NULL, 1, 90)
ON DUPLICATE KEY UPDATE name = VALUES(name);
