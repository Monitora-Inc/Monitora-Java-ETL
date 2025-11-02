package etl;

import java.sql.*;
import java.util.*;

public class ParametroDAO {
    private Connection conn;

    public ParametroDAO(Connection conn) {
        this.conn = conn;
    }

    public Map<String, Map<String, Integer>> buscarLimites(String  idServidor) {
        Map<String, Integer> limitesCriticos = new HashMap<>();
        Map<String, Integer> limitesAtencao = new HashMap<>();
        Map<String, Map<String, Integer>> resultLimites = new HashMap<>();


        String sqlCritico = """
            SELECT nc.componente AS nome_componente, pc.limite 
            FROM componentes_monitorados cm
            JOIN parametros_critico pc ON cm.parametros_critico_id = pc.id
            JOIN nome_componente nc ON cm.nome_componente_id = nc.id
            JOIN servidores s ON cm.servidores_idServidor = s.idServidor
            WHERE s.idServidor = ?;
        """;

        try (PreparedStatement ps = conn.prepareStatement(sqlCritico)) {
            ps.setString(1, idServidor);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                 limitesCriticos.put(rs.getString("nome_componente").toUpperCase(),
                               rs.getInt("limite"));
            }
        } catch (SQLException e) {
            System.out.println("Erro ao buscar limites críticos: " + e);
        }

        //LIMITES DE ATENÇÃO

         String sqlAtencao = """
            SELECT nc.componente AS nome_componente, pa.limite 
            FROM componentes_monitorados cm
            JOIN parametros_atencao pa ON cm.parametros_atencao_id = pa.id
            JOIN nome_componente nc ON cm.nome_componente_id = nc.id
            JOIN servidores s ON cm.servidores_idServidor = s.idServidor
            WHERE s.idServidor = ?;
        """;

        try (PreparedStatement ps = conn.prepareStatement(sqlAtencao)) {
            ps.setString(1, idServidor);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
               limitesAtencao.put(rs.getString("nome_componente").toUpperCase(),
                               rs.getInt("limite"));
            }
        } catch (SQLException e) {
            System.out.println("Erro ao buscar limites de atenção: " + e);
        }

        resultLimites.put("critico", limitesCriticos);
        resultLimites.put("atencao", limitesAtencao);


        return resultLimites;
    }
}
