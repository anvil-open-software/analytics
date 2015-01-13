package com.dematic.labs.persistence.entities;

import com.dematic.labs.persistence.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;

public class PrincipalTest {

    private CrudService crudService;

    @Rule
    public JpaRule jpaRule = new JpaRule(new EmbeddedH2(), "NewPersistenceUnit");

    @Before
    public void setUp() throws Exception {
        crudService = new CrudService(jpaRule.getEntityManager());
    }

    @Test
    public void testSave() {

        Principal principal = new Principal();
        assertNull(principal.getId());

        principal.setUsername("Fred");

        crudService.create(principal);
        assertNotNull(principal.getId());

        jpaRule.changeTransaction();

        Principal principalFromDb = crudService.findExisting(principal.getId(), Principal.class);
        assertEquals(principal.getId(), principalFromDb.getId());
        assertEquals("Fred", principalFromDb.getUsername());
    }

}
