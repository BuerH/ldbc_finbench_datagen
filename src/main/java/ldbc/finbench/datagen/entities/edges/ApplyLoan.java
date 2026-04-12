/*
 * Copyright © 2022 Linked Data Benchmark Council (info@ldbcouncil.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import java.util.List;
import ldbc.finbench.datagen.entities.DynamicActivity;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.Person;
import ldbc.finbench.datagen.entities.nodes.PersonOrCompany;
import ldbc.finbench.datagen.generation.dictionary.Dictionaries;
import ldbc.finbench.datagen.util.RandomGeneratorFarm;

public class ApplyLoan implements DynamicActivity, Serializable {
    private final long ownerId;
    private final PersonOrCompany ownerType;
    private final long loanId;
    private final long creationDate;
    private final long deletionDate;
    private final boolean isExplicitlyDeleted;
    private final String organization;
    private final String comment;
    private final double loanAmount;

    public ApplyLoan(long ownerId, PersonOrCompany ownerType, long loanId, long creationDate, long deletionDate,
                     boolean isExplicitlyDeleted, String organization, String comment, double loanAmount) {
        this.ownerId = ownerId;
        this.ownerType = ownerType;
        this.loanId = loanId;
        this.creationDate = creationDate;
        this.deletionDate = deletionDate;
        this.isExplicitlyDeleted = isExplicitlyDeleted;
        this.organization = organization;
        this.comment = comment;
        this.loanAmount = loanAmount;
    }

    public static void createApplyLoan(RandomGeneratorFarm farm, long creationDate, Person person, Loan loan) {
        loan.setOwnerType(PersonOrCompany.PERSON);
        loan.setOwnerPersonId(person.getPersonId());
        person.addLoan(loan);

        List<Account> poa = person.getAccount();
        loan.setAccounts(poa.toArray(new Account[0]));

        String organization = Dictionaries.loanOrganizations.getUniformDistRandomText(
            farm.get(RandomGeneratorFarm.Aspect.PERSON_APPLY_LOAN_ORGANIZATION));
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));

        ApplyLoan applyLoan =
            new ApplyLoan(person.getPersonId(),
                          PersonOrCompany.PERSON,
                          loan.getLoanId(),
                          creationDate,
                          0,
                          false,
                          organization,
                          comment,
                          loan.getLoanAmount());
        person.getApplyLoans().add(applyLoan);
    }

    public static void createApplyLoan(RandomGeneratorFarm farm, long creationDate, Company company, Loan loan) {
        loan.setOwnerType(PersonOrCompany.COMPANY);
        loan.setOwnerCompanyId(company.getCompanyId());
        company.addLoan(loan);

        List<Account> coa = company.getAccount();
        loan.setAccounts(coa.toArray(new Account[0]));

        String organization = Dictionaries.loanOrganizations.getUniformDistRandomText(
            farm.get(RandomGeneratorFarm.Aspect.COMPANY_APPLY_LOAN_ORGANIZATION));
        String comment =
            Dictionaries.randomTexts.getUniformDistRandomTextForComments(
                farm.get(RandomGeneratorFarm.Aspect.COMMON_COMMENT));

        ApplyLoan applyLoan =
            new ApplyLoan(company.getCompanyId(),
                          PersonOrCompany.COMPANY,
                          loan.getLoanId(),
                          creationDate,
                          0,
                          false,
                          organization,
                          comment,
                          loan.getLoanAmount());
        company.getApplyLoans().add(applyLoan);
    }

    public long getOwnerId() {
        return ownerId;
    }

    public PersonOrCompany getOwnerType() {
        return ownerType;
    }

    public long getLoanId() {
        return loanId;
    }

    @Override
    public long getCreationDate() {
        return creationDate;
    }

    @Override
    public long getDeletionDate() {
        return deletionDate;
    }

    @Override
    public boolean isExplicitlyDeleted() {
        return isExplicitlyDeleted;
    }

    public String getOrganization() {
        return organization;
    }

    public String getComment() {
        return comment;
    }

    public double getLoanAmount() {
        return loanAmount;
    }
}
