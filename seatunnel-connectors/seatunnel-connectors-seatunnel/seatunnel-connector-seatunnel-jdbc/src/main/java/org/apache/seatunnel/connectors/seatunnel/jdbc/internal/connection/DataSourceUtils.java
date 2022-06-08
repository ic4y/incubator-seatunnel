package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection;

import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.sun.tools.javac.comp.Check;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectorOptions;

import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import javax.sql.XADataSource;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @Author: Liuli
 * @Date: 2022/6/7 15:16
 */
public class DataSourceUtils
        implements Serializable
{
    private static final String GETTER_PREFIX = "get";

    private static final String SETTER_PREFIX = "set";

    public static CommonDataSource buildCommonDataSource(JdbcConnectorOptions jdbcConnectorOptions)
            throws InvocationTargetException, IllegalAccessException
    {
        CommonDataSource dataSource = (CommonDataSource) loadDataSource(jdbcConnectorOptions.getXaDataSourceClassName());
        setProperties(dataSource, buildDatabaseAccessConfig(jdbcConnectorOptions));
        return dataSource;
    }

    private static Map<String, Object> buildDatabaseAccessConfig(JdbcConnectorOptions jdbcConnectorOptions)
    {
        HashMap<String, Object> accessConfig = new HashMap<>();
        accessConfig.put("url", jdbcConnectorOptions.getUrl());
        accessConfig.put("user", jdbcConnectorOptions.getUsername());
        accessConfig.put("password", jdbcConnectorOptions.getPassword());

        return accessConfig;
    }

    private static void setProperties(final CommonDataSource commonDataSource, final Map<String, Object> databaseAccessConfig)
            throws InvocationTargetException, IllegalAccessException
    {
        for (Map.Entry<String, Object> entry : databaseAccessConfig.entrySet()) {
            Optional<Method> method = findSetterMethod(commonDataSource.getClass().getMethods(), entry.getKey());
            if (method.isPresent()) {
                method.get().invoke(commonDataSource, entry.getValue());
            }
        }
    }

    private static Method findGetterMethod(final DataSource dataSource, final String propertyName)
            throws NoSuchMethodException
    {
        String getterMethodName = GETTER_PREFIX + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, propertyName);
        Method result = dataSource.getClass().getMethod(getterMethodName);
        result.setAccessible(true);
        return result;
    }

    private static Optional<Method> findSetterMethod(final Method[] methods, final String property)
    {
        String setterMethodName = SETTER_PREFIX + CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, property);
        return Arrays.stream(methods)
                .filter(each -> each.getName().equals(setterMethodName) && 1 == each.getParameterTypes().length)
                .findFirst();
    }

    private static Object loadDataSource(final String xaDataSourceClassName)
    {
        Class<?> xaDataSourceClass;
        try {
            xaDataSourceClass = Thread.currentThread().getContextClassLoader().loadClass(xaDataSourceClassName);
        }
        catch (final ClassNotFoundException ignored) {
            try {
                xaDataSourceClass = Class.forName(xaDataSourceClassName);
            }
            catch (final ClassNotFoundException ex) {
                throw new RuntimeException("Failed to load [" + xaDataSourceClassName + "]", ex);
            }
        }
        try {
            return xaDataSourceClass.getDeclaredConstructor().newInstance();
        }
        catch (final ReflectiveOperationException ex) {
            throw new RuntimeException("Failed to instance [" + xaDataSourceClassName + "]", ex);
        }
    }
}
