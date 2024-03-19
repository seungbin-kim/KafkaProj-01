package com.practice.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;

public class OrderDBHandler {
    public static final Logger logger = LoggerFactory.getLogger(OrderDBHandler.class.getName());

    private Connection connection = null;

    private PreparedStatement insertPrepared = null;

    private static final String INSERT_ORDER_SQL = "INSERT INTO public.orders " +
            "(ord_id, shop_id, menu_name, user_name, phone_number, address, order_time) "+
            "values (?, ?, ?, ?, ?, ?, ?)";

    public OrderDBHandler(String url, String user, String password) {
        try {
            this.connection = DriverManager.getConnection(url, user, password);
            this.insertPrepared = this.connection.prepareStatement(INSERT_ORDER_SQL);
        } catch(SQLException e) {
            logger.error(e.getMessage());
        }

    }

    public void insertOrder(OrderDTO orderDTO)  {
        try {

            setParameter(orderDTO);

            insertPrepared.executeUpdate();
        } catch(SQLException e) {
            logger.error(e.getMessage());
        }

    }

    public void insertOrders(List<OrderDTO> orders) {
        try {
            for(OrderDTO orderDTO : orders) {
                setParameter(orderDTO);
                insertPrepared.addBatch();
            }
            insertPrepared.executeUpdate();

        } catch(SQLException e) {
            logger.info(e.getMessage());
        }

    }


    private void setParameter(OrderDTO orderDTO) throws SQLException {

        insertPrepared.setString(1, orderDTO.orderId);
        insertPrepared.setString(2, orderDTO.shopId);
        insertPrepared.setString(3, orderDTO.menuName);
        insertPrepared.setString(4, orderDTO.userName);
        insertPrepared.setString(5, orderDTO.phoneNumber);
        insertPrepared.setString(6, orderDTO.address);
        insertPrepared.setTimestamp(7, Timestamp.valueOf(orderDTO.orderTime));
    }

    public void close()
    {
        try {
            logger.info("###### OrderDBHandler is closing");
            this.insertPrepared.close();
            this.connection.close();
        }catch(SQLException e) {
            logger.error(e.getMessage());
        }
    }

}