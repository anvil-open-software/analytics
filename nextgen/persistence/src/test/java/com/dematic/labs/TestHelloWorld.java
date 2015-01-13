package com.dematic.labs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestHelloWorld {

    @Test
    public void testHelloWorld() {
        SaaSHelloWorld underTest = new SaaSHelloWorld();
        assertEquals("Hello World", underTest.getGreeting());

    }
}
