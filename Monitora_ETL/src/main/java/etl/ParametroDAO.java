package etl;

import java.sql.*;
import java.util.*;

public class ParametroDAO {
    private Connection conn;

    public ParametroDAO(Connection conn) {
        this.conn = conn;
    }

    public Map<String, Integer> buscarLimites(int idServidor) {
        Map<String, Integer> limites = new HashMap<>();
        String sql = """
            SELECT nc.componente AS nome_componente, p.limite
            FROM componentes_monitorados cm
            JOIN parametros p ON cm.parametros_id = p.id
            JOIN nome_componente nc ON cm.nome_componente_id = nc.id
            JOIN servidores s ON cm.servidores_idServidor = s.idServidor
            WHERE s.idServidor = ?;
        """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, idServidor);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String nome = rs.getString("nome_componente").toUpperCase();
                int limite = rs.getInt("limite");
                limites.put(nome, limite);
            }
        } catch (SQLException e) {
            System.out.println("Erro ao buscar limites: " + e);
        }

        return limites;
    }
}
