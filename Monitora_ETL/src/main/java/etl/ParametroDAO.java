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
            SELECT concat(nc.componente,um.unidade_de_medida) as nome_componente, pc.limite * 1000 AS limite
             FROM componentes_monitorados cm
             JOIN parametros_critico pc ON cm.parametros_critico_id = pc.id
             JOIN nome_componente nc ON cm.nome_componente_id = nc.id
             JOIN servidores s ON cm.servidores_idServidor = s.idServidor
             JOIN unidade_medida um ON cm.unidade_medida_id = um.id
             WHERE s.idServidor = ?;
        """;

        try (PreparedStatement ps = conn.prepareStatement(sqlCritico)) {
            ps.setString(1, idServidor);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                 limitesCriticos.put(rs.getString("nome_componente"),
                               rs.getInt("limite"));
            }
        } catch (SQLException e) {
            System.out.println("Erro ao buscar limites críticos: " + e.getMessage());
        }

        //LIMITES DE ATENÇÃO

         String sqlAtencao = """
            SELECT concat(nc.componente,um.unidade_de_medida) as nome_componente, pa.limite * 1000 AS limite
             FROM componentes_monitorados cm
             JOIN parametros_atencao pa ON cm.parametros_critico_id = pa.id
             JOIN nome_componente nc ON cm.nome_componente_id = nc.id
             JOIN servidores s ON cm.servidores_idServidor = s.idServidor
             JOIN unidade_medida um ON cm.unidade_medida_id = um.id
             WHERE s.idServidor = ?;
        """;

        try (PreparedStatement ps = conn.prepareStatement(sqlAtencao)) {
            ps.setString(1, idServidor);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
               limitesAtencao.put(rs.getString("nome_componente"),
                               rs.getInt("limite"));
            }
        } catch (SQLException e) {
            System.out.println("Erro ao buscar limites de atenção: " + e.getMessage());
        }

        resultLimites.put("critico", limitesCriticos);
        resultLimites.put("atencao", limitesAtencao);


        return resultLimites;
    }
}
