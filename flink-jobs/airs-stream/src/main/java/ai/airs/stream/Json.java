package ai.airs.stream;

import ai.airs.stream.model.AiEvent;
import ai.airs.stream.model.EnrichedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** JSON payloads for the verdicts, new-assets and DLQ topics. */
final class Json {
    private static final ObjectMapper M = new ObjectMapper();

    private Json() {}

    static String verdict(EnrichedEvent e) {
        ObjectNode n = M.createObjectNode();
        n.put("event_id", e.eventId);
        n.put("ts_event", e.tsEvent);
        n.put("device_id", e.deviceId);
        n.put("identity", e.identity);
        n.put("event_type", e.eventType);
        n.put("asset_id", e.assetId);
        n.put("asset_known", e.assetKnown);
        n.put("airs_score", e.airsScoreAtEvent);
        n.put("band", e.bandAtEvent);
        n.put("sanctioned", e.sanctionedAtEvent);
        n.put("catalog_valid_from", e.catalogValidFrom);
        n.putPOJO("sensitive_types", e.sensitiveTypes);
        n.put("policy_id", e.policyId);
        n.put("action", e.action);
        n.put("latency_ms", e.latencyMs);
        return n.toString();
    }

    static String newAsset(AiEvent e) {
        ObjectNode n = M.createObjectNode();
        n.put("asset_ref", e.assetRef);
        n.put("first_seen", e.tsEvent);
        n.put("device_id", e.deviceId);
        n.put("event_type", e.eventType);
        return n.toString();
    }

    static String deadLetter(AiEvent e) {
        ObjectNode n = M.createObjectNode();
        n.put("error", e.parseError);
        n.put("kafka_timestamp", e.tsEvent);
        n.put("raw_base64", e.rawBase64);
        return n.toString();
    }
}
