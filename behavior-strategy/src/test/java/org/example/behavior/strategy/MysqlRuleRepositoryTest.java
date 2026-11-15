package org.example.behavior.strategy;

import org.example.behavior.common.enums.CouponType;
import org.example.behavior.common.enums.UserLevel;
import org.example.behavior.domain.rule.MarketingRule;
import org.example.behavior.strategy.repository.MysqlRuleRepository;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test against a local MySQL 8 demonstrating the full CRUD cycle
 * of {@link MysqlRuleRepository}: 增 (insert), 改 (update), 删 (delete) plus
 * 查 (findAll / findById).
 *
 * <p>Skipped automatically when {@code MYSQL_PASSWORD} is not set, so plain
 * {@code mvn verify} without a local MySQL still passes.
 */
class MysqlRuleRepositoryTest {

    private static final String TEST_RULE_A = "test-rule-crud-a";
    private static final String TEST_RULE_B = "test-rule-crud-b";

    private static MysqlRuleRepository repository;
    private static String url;

    @BeforeAll
    static void setUp() throws Exception {
        String password = System.getenv("MYSQL_PASSWORD");
        assumeTrue(password != null && !password.isBlank(),
                "MYSQL_PASSWORD not set; skipping MySQL CRUD integration test");

        url = "jdbc:mysql://localhost:3306/user_behavior"
                + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
        repository = new MysqlRuleRepository(url, "root", password);
        bootstrapSchema(password);
        cleanupTestData();
    }

    @AfterAll
    static void tearDown() {
        cleanupTestData();
    }

    private static void bootstrapSchema(String password) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, "root", password);
             Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS marketing_rule ("
                    + "id VARCHAR(64) NOT NULL PRIMARY KEY,"
                    + "name VARCHAR(128) NOT NULL,"
                    + "pattern_name VARCHAR(64) NOT NULL,"
                    + "coupon_template_id VARCHAR(64) NOT NULL,"
                    + "coupon_type VARCHAR(16) NOT NULL,"
                    + "cooldown_seconds BIGINT NOT NULL DEFAULT 0,"
                    + "audience_levels JSON NULL,"
                    + "enabled TINYINT(1) NOT NULL DEFAULT 1,"
                    + "priority INT NOT NULL DEFAULT 0,"
                    + "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
                    + ") ENGINE=InnoDB");
        }
    }

    private static void cleanupTestData() {
        if (repository == null) {
            return;
        }
        repository.delete(TEST_RULE_A);
        repository.delete(TEST_RULE_B);
    }

    private static MarketingRule rule(String id, String couponId, int priority, boolean enabled,
                                      Set<UserLevel> audience) {
        return MarketingRule.builder()
                .id(id)
                .name("CRUD 集成测试规则 " + id)
                .patternName("*")
                .couponTemplateId(couponId)
                .couponType(CouponType.CASH)
                .cooldownSeconds(60)
                .audienceLevels(audience)
                .enabled(enabled)
                .priority(priority)
                .build();
    }

    @Test
    void fullCrudCycle() {
        // ---- 查: seed rules exist ----
        List<MarketingRule> all = repository.findAll();
        assertTrue(all.size() >= 2, "seed rules should be present");
        assertTrue(repository.findById("rule-cart-abandon").isPresent());

        // ---- 增: insert ----
        assertEquals(1, repository.insert(
                rule(TEST_RULE_A, "CPN_TEST_1", 10, true, null)));
        Optional<MarketingRule> inserted = repository.findById(TEST_RULE_A);
        assertTrue(inserted.isPresent());
        assertEquals("CPN_TEST_1", inserted.get().getCouponTemplateId());
        assertTrue(inserted.get().getAudienceLevels() == null
                || inserted.get().getAudienceLevels().isEmpty());

        // insert with audience levels round trip (JSON column)
        assertEquals(1, repository.insert(
                rule(TEST_RULE_B, "CPN_TEST_2", 5, false, Set.of(UserLevel.GOLD, UserLevel.PLATINUM))));
        Set<UserLevel> audience = repository.findById(TEST_RULE_B).orElseThrow().getAudienceLevels();
        assertEquals(Set.of(UserLevel.GOLD, UserLevel.PLATINUM), audience);

        // ---- 改: update ----
        assertEquals(1, repository.update(
                rule(TEST_RULE_A, "CPN_TEST_1_UPDATED", 99, false, Set.of(UserLevel.SILVER))));
        MarketingRule updated = repository.findById(TEST_RULE_A).orElseThrow();
        assertEquals("CPN_TEST_1_UPDATED", updated.getCouponTemplateId());
        assertEquals(99, updated.getPriority());
        assertFalse(updated.isEnabled());
        assertEquals(Set.of(UserLevel.SILVER), updated.getAudienceLevels());

        // ---- 删: delete ----
        assertEquals(1, repository.delete(TEST_RULE_A));
        assertTrue(repository.findById(TEST_RULE_A).isEmpty());
        assertEquals(0, repository.delete(TEST_RULE_A), "deleting again affects 0 rows");

        assertEquals(1, repository.delete(TEST_RULE_B));
        assertFalse(repository.findAll().stream()
                .anyMatch(r -> r.getId().startsWith("test-rule-crud")));
    }

    @Test
    void insertDuplicateFails() {
        repository.insert(rule(TEST_RULE_A, "CPN_TEST_1", 10, true, null));
        org.junit.jupiter.api.Assertions.assertThrows(
                org.example.behavior.common.exception.BehaviorException.class,
                () -> repository.insert(rule(TEST_RULE_A, "CPN_TEST_1", 10, true, null)));
        repository.delete(TEST_RULE_A);
    }
}
