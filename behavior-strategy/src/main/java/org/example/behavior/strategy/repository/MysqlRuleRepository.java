package org.example.behavior.strategy.repository;

import com.fasterxml.jackson.core.type.TypeReference;
import org.example.behavior.common.enums.CouponType;
import org.example.behavior.common.enums.UserLevel;
import org.example.behavior.common.exception.BehaviorException;
import org.example.behavior.common.util.JsonUtils;
import org.example.behavior.domain.rule.MarketingRule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * MySQL 8 backed {@link RuleRepository} with full CRUD, keeping the strategy
 * module Flink free. Credentials are supplied by the caller; the job resolves
 * them from config / the {@code MYSQL_PASSWORD} environment variable, never
 * from a file in the repository.
 *
 * <p>One short lived JDBC connection per operation keeps the class safe under
 * Flink's parallel subtasks without a connection pool. {@link #findAll()} is
 * called once per strategy decision in the static-rules mode, so prefer the
 * broadcast-rules mode for production volumes.
 */
public class MysqlRuleRepository implements RuleRepository {

    private static final long serialVersionUID = 1L;

    private static final String COLUMNS =
            "id, name, pattern_name, coupon_template_id, coupon_type, "
                    + "cooldown_seconds, audience_levels, enabled, priority";

    private static final String SELECT_ALL = "SELECT " + COLUMNS + " FROM marketing_rule";

    private static final String INSERT =
            "INSERT INTO marketing_rule (id, name, pattern_name, coupon_template_id, coupon_type, "
                    + "cooldown_seconds, audience_levels, enabled, priority) VALUES (?,?,?,?,?,?,?,?,?)";

    /** id appears both in SET and WHERE so one bind sequence serves both insert and update. */
    private static final String UPDATE =
            "UPDATE marketing_rule SET id=?, name=?, pattern_name=?, coupon_template_id=?, coupon_type=?, "
                    + "cooldown_seconds=?, audience_levels=?, enabled=?, priority=? WHERE id=?";

    private static final String DELETE = "DELETE FROM marketing_rule WHERE id=?";

    private final String url;
    private final String user;
    private final String password;

    public MysqlRuleRepository(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public List<MarketingRule> findAll() {
        List<MarketingRule> rules = new ArrayList<>();
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(SELECT_ALL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                rules.add(mapRow(rs));
            }
            return rules;
        } catch (SQLException e) {
            throw new BehaviorException("Failed to load marketing rules from MySQL", e);
        }
    }

    /** 新增规则；主键冲突抛出异常。 */
    public int insert(MarketingRule rule) {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(INSERT)) {
            bindColumns(ps, rule, 1);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new BehaviorException("Failed to insert marketing rule " + rule.getId(), e);
        }
    }

    /** 修改规则（按 id 覆盖）；规则不存在时返回 0 行。 */
    public int update(MarketingRule rule) {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(UPDATE)) {
            int index = bindColumns(ps, rule, 1);
            ps.setString(index, rule.getId());
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new BehaviorException("Failed to update marketing rule " + rule.getId(), e);
        }
    }

    /** 删除规则；规则不存在时返回 0 行。 */
    public int delete(String ruleId) {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(DELETE)) {
            ps.setString(1, ruleId);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new BehaviorException("Failed to delete marketing rule " + ruleId, e);
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    /** Binds the rule columns starting at {@code startIndex}; returns the next free index. */
    private static int bindColumns(PreparedStatement ps, MarketingRule rule, int startIndex)
            throws SQLException {
        int i = startIndex;
        ps.setString(i++, rule.getId());
        ps.setString(i++, rule.getName());
        ps.setString(i++, rule.getPatternName());
        ps.setString(i++, rule.getCouponTemplateId());
        ps.setString(i++, rule.getCouponType() == null ? null : rule.getCouponType().name());
        ps.setLong(i++, rule.getCooldownSeconds());
        ps.setString(i++, audienceToJson(rule.getAudienceLevels()));
        ps.setBoolean(i++, rule.isEnabled());
        ps.setInt(i++, rule.getPriority());
        return i;
    }

    private static MarketingRule mapRow(ResultSet rs) throws SQLException {
        return MarketingRule.builder()
                .id(rs.getString("id"))
                .name(rs.getString("name"))
                .patternName(rs.getString("pattern_name"))
                .couponTemplateId(rs.getString("coupon_template_id"))
                .couponType(parseCouponType(rs.getString("coupon_type")))
                .cooldownSeconds(rs.getLong("cooldown_seconds"))
                .audienceLevels(jsonToAudience(rs.getString("audience_levels")))
                .enabled(rs.getBoolean("enabled"))
                .priority(rs.getInt("priority"))
                .build();
    }

    private static CouponType parseCouponType(String code) {
        return code == null || code.isBlank() ? null : CouponType.valueOf(code.trim());
    }

    private static String audienceToJson(Set<UserLevel> levels) {
        if (levels == null || levels.isEmpty()) {
            return null;
        }
        List<String> names = new ArrayList<>();
        for (UserLevel level : levels) {
            names.add(level.name());
        }
        return JsonUtils.toJson(names);
    }

    private static Set<UserLevel> jsonToAudience(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            List<String> names = JsonUtils.mapper().readValue(json, new TypeReference<List<String>>() {
            });
            Set<UserLevel> levels = new LinkedHashSet<>();
            for (String name : names) {
                levels.add(UserLevel.valueOf(name));
            }
            return levels;
        } catch (Exception e) {
            throw new BehaviorException("Failed to parse audience_levels JSON: " + json, e);
        }
    }
}
