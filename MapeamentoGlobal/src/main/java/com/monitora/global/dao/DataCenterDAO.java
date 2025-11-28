package com.monitora.global.dao;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DataCenterDAO {
    private final Connection conn;

    public DataCenterDAO(Connection conn) {
        this.conn = conn;
    }

    public Integer buscarDataCenterPorServidor(String idServidor) throws Exception {

        String sql = """
                SELECT FkDataCenter
                FROM servidores
                WHERE idServidor = ?
                """;

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, idServidor);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("FkDataCenter");
                }
            }
        }
        return null;
    }
}
