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

    public Double buscarLimites(Integer idServidor) throws Exception {
        String sql = "select AVG(limite) from monitora.parametros_critico pc\n" +
                "join componentes_monitorados cm ON cm.parametros_critico_id = pc.id\n" +
                "join Servidores s on s.idServidor = cm.servidores_idServidor\n" +
                "where cm.nome_componente_id = 4 AND unidade_medida_id = 2 AND s.fkDataCenter = ?;";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, idServidor);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getDouble("limite");
                }
            }
        }
        return null;
    }
}
