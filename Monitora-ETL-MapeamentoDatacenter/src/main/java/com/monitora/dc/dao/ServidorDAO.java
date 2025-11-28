package com.monitora.dc.dao;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ServidorDAO {
    private final Connection conn;
    public ServidorDAO(Connection conn) {
        this.conn = conn;
    }

    // 1) Buscar o id do DataCenter do uuid de um servidor
    public Integer buscarDataCenterPorServidor(String idServidor) throws SQLException {
        String sql = "SELECT FkDataCenter FROM servidores WHERE idServidor = ?";
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